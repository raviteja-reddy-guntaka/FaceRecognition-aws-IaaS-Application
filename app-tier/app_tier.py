# -------------------------------- Imports ----------------------------------------
import os
import time

import torch
import torchvision.transforms as transforms
import torchvision.models as models
import numpy as np
import json
import boto3
from PIL import Image
import csv
import logging


logging.basicConfig(filename='/home/ubuntu/info.log', filemode='w', encoding='utf-8', level=logging.INFO)
logging.info('App tier')


# This function uploads the result txt file to Input S3 Bucket
def upload_classification_file_to_s3(file_name_txt, file_path):
    logging.info("Uploading file to S3 bucket", file_name_txt)
    try:
        s3_client.upload_file(file_path, S3_OUTPUT_BUCKET, file_name_txt)
        logging.info("uploaded to S3")
    except Exception as upload_exception:
        logging.error(upload_exception)
        logging.error("Error while Uploading file to S3")


# this creates a file with file name and class name with comma separated
def create_result_file(file_name, class_name):
    logging.info("creating result file to upload")
    base_path = "/home/ubuntu/"
    file_name_txt = file_name.split('.')[0] + ".txt"
    with open(base_path + file_name_txt, "w", encoding='utf-8') as temp_file:
        logging.info("started writing")
        result_writer = csv.writer(temp_file, delimiter=',')
        result_writer.writerow([file_name, class_name])
        temp_file.close()
    logging.info("created result file")
    os.chmod(base_path + file_name_txt, 0o777)
    logging.info("changed file permissions")
    upload_classification_file_to_s3(file_name_txt, base_path + file_name_txt)


# This function fetch the output file from Output S3 Bucket
def get_image_from_s3(file_name):
    logging.info("Fetching image file from S3 : {}".format(file_name))
    return s3_client.get_object(Bucket=S3_INPUT_BUCKET, Key=file_name)["Body"]


def classify_image(file_name):
    image_file_stream = get_image_from_s3(file_name)
    im = Image.open(image_file_stream)
    img = np.array(im)
    # img = Image.open(urlopen(BUCKET_URL + file_name))
    model = models.resnet18(pretrained=True)
    model.eval()
    img_tensor = transforms.ToTensor()(img).unsqueeze_(0)
    outputs = model(img_tensor)
    _, predicted = torch.max(outputs.data, 1)

    with open('/home/ubuntu/app-tier/imagenet-labels.json') as f:
        labels = json.load(f)
    result = labels[np.array(predicted)[0]]
    return result


# ---------------------------------------- Image Processing ----------------------------------------


# Helper function to delete message for SQS after it has been processed
def delete_message_from_sqs(receipt_handle):
    sqs_client.delete_message(QueueUrl=SQS_REQUEST_QUEUE_URL, ReceiptHandle=receipt_handle)


# Helper function to create payload for SQS
def get_sqs_request_attributes(MessageAttributes, class_name):
    MessageAttributes["CLASS_NAME"] = {"DataType": "String", "StringValue": class_name}
    return MessageAttributes


# Publish message to Response Queue
def publish_to_sqs(filename, MessageAttributes):
    response = sqs_client.send_message(QueueUrl=SQS_RESPONSE_QUEUE_URL, MessageAttributes=MessageAttributes,
                                       MessageBody=filename)
    logging.info(response['MessageId'])


# * This function consume message from SQS_REQUEST_QUEUE_URL, extract the file name from message attributes
# * Then download the image from Input S3 Bucket and use the default model to classify the image.
# * Once Image is classified, It creates a result file and uploads it to Output S3 Bucket.
# * After that it publish the filename and class name to Response Queue.
# * Once Message is processed, it is deleted it from the Request Queue.
# * Only one image is processed at a time
def consume_from_sqs():
    while True:
        logging.info("Consuming Messages")
        response = sqs_client.receive_message(QueueUrl=SQS_REQUEST_QUEUE_URL,
                                              MessageAttributeNames=["All"],
                                              MaxNumberOfMessages=1,
                                              WaitTimeSeconds=10)
        if 'Messages' in response:
            message = response['Messages'][0]
            logging.info("processing message with id : {}".format(message['MessageId']))
            MessageAttributes = message["MessageAttributes"]
            filename = MessageAttributes["FILE_NAME"]["StringValue"]
            logging.info("Starting classification for {}".format(filename))
            class_name = classify_image(filename)
            logging.info("Classification for {} ended {}".format(filename, class_name))
            MessageAttributes = get_sqs_request_attributes(MessageAttributes, class_name)
            create_result_file(filename, class_name)
            publish_to_sqs(filename, MessageAttributes)
            logging.info("deleting message from queue".format(message['MessageId']))
            delete_message_from_sqs(message["ReceiptHandle"])
        time.sleep(5)


# -------------------------------- SQS Related ----------------------------------------

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
REGION = 'us-east-1'

# Making the application look for messages on SQS


# if __name__ == '__main__':
sqs_client = boto3.client('sqs',
                          region_name=REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
# sqs_client = boto3.client('sqs')
SQS_REQUEST_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/637104051619/RequestMessageQueue"
SQS_RESPONSE_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/637104051619/ResponseMessageQueue"

# -------------------------------- S3 Related ----------------------------------------
# Setting up Bucket name
S3_INPUT_BUCKET = "cloud-p1-input-bucket"
S3_OUTPUT_BUCKET = "cloud-p1-output-bucket"

# Create S3 Client for fetching image and uploading the txt result file
s3_client = boto3.client('s3',
                         region_name=REGION,
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

while True:
    try:
        consume_from_sqs()
    except Exception as e:
        logging.error("Error while consuming messages from Request Queue and processing it. {}".format(e))
