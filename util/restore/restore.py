# restore.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
from configparser import ConfigParser
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import json
import logging
# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))

# Get configuration
config = ConfigParser(os.environ)
config.read('restore_config.ini')

# Add utility code here


def s3_config():
    my_config = Config(
        region_name=config['aws']['AwsRegionName'],
        signature_version=config['aws']['SignatureVersion'],
    )
    return my_config


def sqs_poll_message(sqs):

    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-long-polling.html#enable-long-polling-on-message-receipt
    try:
        response = sqs.receive_message(
            QueueUrl=config['sqs']['QueueUrl'],
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=int(config['sqs']['MaxNumberOfMessages']),
            MessageAttributeNames=[
                'All'
            ],
            WaitTimeSeconds=int(config['sqs']['WaitTimeSeconds'])
        )
    except ClientError:
        print("Fail to poll message from sqs queue")
        return None, None
    if "Messages" not in response:
        print("No new message in message queue")
        return None, None
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
    message = response['Messages'][0]

    receipt_handle = message['ReceiptHandle']
    data_content = json.loads(json.loads(message['Body'])['Message'])

    return data_content, receipt_handle


def dynamo_query(table_name, user_id):
    # Reference: Canvas modules class5 dynamo_reader.py
    my_config = s3_config()
    try:
        dynamo = boto3.resource('dynamodb', config=my_config)
        table = dynamo.Table(table_name)
        response = table.query(
            IndexName='user_id_index',
            KeyConditionExpression=Key('user_id').eq(
                user_id)
        )
    except ClientError:
        logging.error("Fail to query items from dynamo database!")
        return None
    return response['Items']


def sqs_delete_messages(sqs, receipt_handle):
    try:
        sqs.delete_message(
            QueueUrl=config['sqs']['QueueUrl'],
            ReceiptHandle=receipt_handle
        )
    except ClientError:
        print("Fail to delete message in sqs")
        return False
    return True


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
def glacier_retrieve_expedited(archiveId, job_id):
    my_config = s3_config()
    try:
        client = boto3.client('glacier', config=my_config)
        response = client.initiate_job(
            accountId='-',
            jobParameters={
                'ArchiveId': archiveId,
                'Description': job_id,
                # notify the topic when the retrieve complete
                'SNSTopic': config['sns']['TopicArnThaw'],
                'Type': 'archive-retrieval',
                'Tier': 'Expedited',
                # 'OutputLocation': {
                #     'S3': {
                #         'BucketName': config['s3']['AWS_S3_RESULTS_BUCKET'],
                #         'Prefix': config['s3']['AWS_S3_KEY_PREFIX'],
                #     }
                # }
            },
            vaultName=config['glacier']['VaultName'],
        )
    except ClientError as e:
        logging.error(e)
        print("Fail to expedited retrieve!")
        return None
    return response


def glacier_retrieve_standard(archiveId, job_id):
    my_config = s3_config()
    try:
        client = boto3.client('glacier', config=my_config)
        response = client.initiate_job(
            accountId='-',
            jobParameters={
                'ArchiveId': archiveId,
                'Description': job_id,
                # notify the topic when the retrieve complete
                'SNSTopic': config['sns']['TopicArnThaw'],
                'Type': 'archive-retrieval',
                'Tier': 'Standard',
                # 'OutputLocation': {
                #     'S3': {
                #         'BucketName': config['s3']['AWS_S3_RESULTS_BUCKET'],
                #         'Prefix': config['s3']['AWS_S3_KEY_PREFIX'],
                #     }
                # }
            },
            vaultName=config['glacier']['VaultName'],
        )
    except ClientError as e:
        logging.error(e)
        print("Fail to standard retrieve!")
        return None
    return response


# def sns_pub_thaw(data):
#     # select the Topic arn to publish
#     topic_arn = config['sns']['TopicArnThaw']
#     data = json.dumps(data)

#     try:
#         my_config = s3_config()
#         client = boto3.client('sns', config=my_config)
#         client.publish(TopicArn=topic_arn, Message=data)
#     except ClientError:
#         print("Fail to publish to sns thaw topic!")
#         return False
#     return True


def main():
    # connect to sqs
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
    my_config = s3_config()
    try:
        sqs = boto3.client('sqs', config=my_config)
    except ClientError:
        print("fail to connect to sqs")
        print("check the status of sqs and restart the program!")
        return

    # keep retrieve message from the queue
    # # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
    while True:
        message, receipt_handle = sqs_poll_message(sqs)

        if message is None and receipt_handle is None:
            continue

        user_id = message['user_id']
        # get the tuples whose 'user_id'=user_id
        user_items = dynamo_query(config['dynamodb']['TableName'], user_id)
        # use archive id to retrieve file
        for user_item in user_items:
            if 'results_file_archive_id' in user_item.keys():
                archiveId = user_item['results_file_archive_id']
                # first attempt to use Expedited retrievals from Glacier
                if archiveId is not None:
                    response_expedited = glacier_retrieve_expedited(
                        archiveId, user_item['job_id'])
                    print(response_expedited)
                    # if Expedited retrieval requests fail
                    if response_expedited is None:
                        # second attempt to use standard retrievals from Glacier
                        response_standard = glacier_retrieve_standard(
                            archiveId, user_item['job_id'])
                        print(response_standard)

        # delete message in queue
        sqs_delete_messages(sqs, receipt_handle)


if __name__ == "__main__":
    main()
# EOF
