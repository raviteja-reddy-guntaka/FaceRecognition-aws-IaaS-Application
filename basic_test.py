import boto3

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
REGION = 'us-east-1'

# Specify your Python script
python_script="""#!/bin/bash
echo "Hello from my script!" > /tmp/hello.txt
su ubuntu -c "usr/bin/python3 /home/ubuntu/app-tier/app_tier.py" &
"""


def create_ec2_instance():
    ec2 = boto3.resource(
        'ec2',
        region_name=REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    ami_id = "ami-0ab315c080885e5df"

    instances = ec2.create_instances(
        ImageId=ami_id,
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        UserData=python_script,
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': 'App Tier Worker'
                    }
                ]
            }
        ]
    )

    return instances[0]


def run_ec2_instance(instance):
    # Start the EC2 instance
    instance.start()


def terminate_ec2_instance(instance):
    # Terminate the EC2 instance
    instance.terminate()


def create_sqs_queue(queue_name):
    # Create an SQS client
    sqs = boto3.client(
        'sqs',
        region_name=REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    # Create an SQS queue
    response = sqs.create_queue(
        QueueName=queue_name,
    )

    # Print the queue URL and ARN
    print(f"Queue URL: {response['QueueUrl']}")


if __name__ == '__main__':
    ec2_instance = create_ec2_instance()
    print(f"Created instance ID: {ec2_instance.id}")

    run_ec2_instance(ec2_instance)
    print("Instance is now running.")

    # # You can call terminateEC2Instance(ec2_instance) when you're ready to terminate the instance.
    # terminateEC2Instance(ec2_instance)

    # Create an SQS queue
    # sqs_queue_name = "MyQueue"  # Replace with your desired queue name
    # create_sqs_queue(sqs_queue_name)

# ami-0f519e8ed772ebcbe
# snap-09dc8ca8ff662387c
