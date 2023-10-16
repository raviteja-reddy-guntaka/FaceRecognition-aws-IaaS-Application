from flask import Flask, request, jsonify
import boto3
import time
import threading
import logging

app = Flask(__name__)

# configuration for logging
logging.basicConfig(filename='/home/ubuntu/web-tier/web_tier.log', filemode='w', encoding='utf-8', level=logging.INFO)
logging.info('Web tier application')

# AWS credentials
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
REGION = 'us-east-1'

# S3 bucket details and required helper methods
S3_INPUT_BUCKET = "cloud-p1-input-bucket"
S3_OUTPUT_BUCKET = "cloud-p1-output-bucket"

# Create S3 Client for uploading image and downloading the txt result file
s3_client_object = boto3.client('s3',
                         region_name=REGION,
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


# This function uploads the image file to Input S3 Bucket
def upload_image_file_to_s3(image_file):
    try:
        s3_client_object.upload_fileobj(image_file, S3_INPUT_BUCKET, image_file.filename, ExtraArgs={})
        logging.info("uploaded to S3 : {0}".format(image_file.filename))
    except Exception as e:
        logging.info("Error while Uploading file to S3. {}".format(e))


# This function fetch the output file from Output S3 Bucket
def get_txt_file_from_s3(file_name):
    logging.info("Fetching result file from S3 {}". format(file_name))
    return s3_client_object.get_object(Bucket=S3_OUTPUT_BUCKET, Key=file_name)["Body"]


# -------------------------------- Local Data store ----------------------------------------
# Using dict to maintain results for requests,
# once message is pushed to Request Queue, user request wait in this loop
# until a response is available in response Queue
# Once a response has been sent, we clear it from the memory
RESULTS = dict()


def get_response_once_available(file_name):
    try:
        while file_name not in RESULTS.keys():
            continue
        class_name = RESULTS[file_name]
        RESULTS.pop(file_name)
        return class_name
    except:
        logging.info("Error while waiting for response from Queue")


# -------------------------------- SQS Related ----------------------------------------
# Create SQS Client for publishing requests to Request Queue which will be consumed by
# App Tier for classification and classification response from Response Queue
sqs_client = boto3.client('sqs',
                          region_name=REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
SQS_REQUEST_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/637104051619/RequestMessageQueue"
SQS_RESPONSE_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/637104051619/ResponseMessageQueue"


# Helper function to delete message for SQS after it has been processed
def delete_message_from_sqs(receipt_handle):
    response = sqs_client.delete_message(QueueUrl=SQS_RESPONSE_QUEUE_URL, ReceiptHandle=receipt_handle)
    logging.info(response)


# Helper function to create payload to be used with SQS_REQUEST_QUEUE_URL
def get_sqs_request_attributes(file_name):
    return {"FILE_NAME": {"DataType": "String", "StringValue": file_name}}


# Publish message to Request Queue
def publish_request_message_to_sqs(filename):
    response = sqs_client.send_message(QueueUrl=SQS_REQUEST_QUEUE_URL,
                                       MessageAttributes=get_sqs_request_attributes(filename),
                                       MessageBody=filename)
    logging.info("Message : {0}".format(response['MessageId']))


# Continuously checking the Response Queue for messages and, when a message becomes available,
# storing the data in a local RESULT dictionary for the waiting thread to access
# and use in responding to the user.
def consume_from_sqs():
    while True:
        try:
            logging.info("Consuming Messages from response queue")
            response = sqs_client.receive_message(QueueUrl=SQS_RESPONSE_QUEUE_URL,
                                                  MessageAttributeNames=["FILE_NAME", "CLASS_NAME"],
                                                  MaxNumberOfMessages=10, WaitTimeSeconds=20)
            if 'Messages' in response:
                logging.info("Response Queue count : {}".format(len(response['Messages'])))
                for message in response['Messages']:
                    logging.info("processing message : {}".format(message['MessageId']))
                    message_attributes = message["MessageAttributes"]
                    # Storing the response to global dict to respond
                    RESULTS[message_attributes["FILE_NAME"]["StringValue"]] = message_attributes["CLASS_NAME"]["StringValue"]
                    delete_message_from_sqs(message["ReceiptHandle"])
                    logging.info("deleting message : {} from  {}".format(message['MessageId'], SQS_RESPONSE_QUEUE_URL))
            time.sleep(5)
        except Exception as e:
            logging.info(e)
            logging.info("Error while consuming message from Response Queue")


# Establishing an independent thread to manage ongoing polling from the Response Queue.
sqs_monitoring_thread = threading.Thread(target=consume_from_sqs)
sqs_monitoring_thread.start()


# This API serves as the gateway for users to submit images for classification.
# It manages user requests, waiting until the app tier publishes the classification outcome to the response queue.
#
# "myfile" is the key using which image is sent
@app.route("/upload", methods=["POST"])
def upload_image():
    try:
        logging.info("Received an image request")
        image = request.files['myfile']
        upload_image_file_to_s3(image)
        logging.info("{} published to request queue".format(image.filename))
        publish_request_message_to_sqs(image.filename)
        logging.info("{} waiting for response".format(image.filename))
        get_response_once_available(image.filename)
        logging.info("{} response received".format(image.filename))
        return get_txt_file_from_s3(image.filename.split('.')[0] + ".txt")
    except Exception as e:
        logging.info("Exception encountered :", e)
        logging.info("Error while Processing user request")


'''
Starting flask app to expose upload REST endpoint
'''
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000, threaded=True)
