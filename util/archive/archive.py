# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
from configparser import ConfigParser
import json
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import logging
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))

# Get configuration
config = ConfigParser(os.environ)
config.read('archive_config.ini')


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
        print("No new message in archive queue")
        return None, None
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
    message = response['Messages'][0]

    receipt_handle = message['ReceiptHandle']
    data_content = json.loads(json.loads(message['Body'])['Message'])

    return data_content, receipt_handle


def get_s3_file(bucket_name, object_name):
    my_config = s3_config()
    try:
        s3 = boto3.resource('s3', config=my_config)
        obj = s3.Object(bucket_name, object_name)
    except ClientError as e:
        print("Fail to read log file in s3!")
        logging(e)
        return None
    return obj


def glacier_archive(s3_obj):
    my_config = s3_config()
    glacier = boto3.client(
        'glacier', config=my_config)
    try:
        response = glacier.upload_archive(vaultName=config['glacier']['VaultName'],
                                          body=s3_obj.get()['Body'].read())
        print("Successfully archive!")
    except ClientError as e:
        print("Fail to upload s3_obj to glacier vault!")
        return None
    return response['archiveId']


def dynamo_update_archive(table_name, job_id, archiveId):
    my_config = s3_config()
    try:
        dynamo = boto3.resource('dynamodb', config=my_config)
        table = dynamo.Table(table_name)
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression="SET results_file_archive_id=:r",
            ExpressionAttributeValues={
                ':r': archiveId,
            },
        )
    except ClientError:
        print("Fail to update archiveID into dynamo database")
        return False
    return True

# https://stackoverflow.com/questions/3140779/how-to-delete-files-from-amazon-s3-bucket


def s3_delete_file(bucket_name, obj_name):
    my_config = s3_config()
    try:
        s3 = boto3.client('s3', config=my_config)
        s3.delete_object(Bucket=bucket_name, Key=obj_name)
    except ClientError:
        print("Fail to delete file given the bucket and key!")
        return False
    return True


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


# Add utility code here


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

        job_id = message['job_id']

        # move file to the glacier
        s3_obj_to_move = get_s3_file(
            message['s3_results_bucket'], message['s3_key_result_file'])
        archiveId = glacier_archive(s3_obj_to_move)

        # update archieveID in dynamodb
        dynamo_update_archive(
            config['dynamodb']['TableName'], job_id, archiveId)

        # delete file in s3
        s3_delete_file(message['s3_results_bucket'],
                       message['s3_key_result_file'])

        # delete message in queue
        sqs_delete_messages(sqs, receipt_handle)


if __name__ == "__main__":
    main()
# EOF
