import boto3
import math
from time import sleep
from collections import defaultdict

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
REGION = 'us-east-1'

# sqs queue information
SQS_REQUEST_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/637104051619/RequestMessageQueue"
SQS_RESPONSE_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/637104051619/ResponseMessageQueue"

# Other configuration parameters
factor = 5
auto_scaling_upper_limit = 20
auto_scaling_lower_limit = 1

# Initialize AWS clients
ec2 = boto3.resource(
    'ec2',
    region_name=REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)
sqs = boto3.client(
    'sqs',
    region_name=REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

terminated_instances = set()
terminate_requests = defaultdict(int)
in_transit = []

# Specify your Python script
# user_data_script = """#!/bin/bash
# echo "Testing" > /tmp/test.txt
# su ubuntu -c "/usr/bin/python3 /home/ubuntu/app-tier/app_tier.py"
# """

app_tier_ami_id = "ami-0eb335b25f6d10761"

while True:
    # Calculate the number of messages in the queue
    response = sqs.get_queue_attributes(
        QueueUrl=SQS_REQUEST_QUEUE_URL,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    no_of_messages_in_queue = int(response['Attributes']['ApproximateNumberOfMessages'])

    # Get the list of currently running EC2 instances
    running_instance = [i.id for i in ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])]
    running_instance.remove("i-0b9ebc12222767b93")

    # Remove instances that are no longer running
    in_transit = [i for i in in_transit if i not in running_instance]

    total_running_instances = len(running_instance) + len(in_transit)

    action = None
    new_scaling = None

    new_ec2_instances_needed = max(auto_scaling_lower_limit, math.ceil(no_of_messages_in_queue / factor)) - total_running_instances
    ratio = min(auto_scaling_upper_limit, new_ec2_instances_needed)

    if ratio > 0:
        action = "SCALE_OUT"
        new_scaling = ratio
    elif ratio < 0:
        action = "SCALE_IN"
        new_scaling = abs(ratio)
    elif no_of_messages_in_queue == 0:
        action = "SCALE_IN"
        new_scaling = ratio

    print(action, " : ", new_scaling)

    if action == "SCALE_OUT":

        c_instances = ec2.create_instances(
            ImageId=app_tier_ami_id,
            MinCount=new_scaling,
            MaxCount=new_scaling,
            InstanceType="t2.micro",
            # UserData=user_data_script,
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
        in_transit.extend([i.id for i in c_instances])

    elif action == "SCALE_IN":
        for i in range(new_scaling):
            if i < len(running_instance) and running_instance[i] not in terminated_instances:
                ec2.instances.filter(InstanceIds=[running_instance[i]]).terminate()
                terminated_instances.add(running_instance[i])

    sleep(10)