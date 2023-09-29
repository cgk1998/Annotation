# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
from configparser import ConfigParser
import helpers
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
config.read('thaw_config.ini')

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
        print("No new message in thaw queue")
        return None, None
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
    message = response['Messages'][0]

    receipt_handle = message['ReceiptHandle']
    data_content = json.loads(json.loads(message['Body'])['Message'])

    return data_content, receipt_handle


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


def download_restored_file(job_id):
    my_config = s3_config()
    try:
        client = boto3.client('glacier', config=my_config)
        response = client.get_job_output(
            accountId='-',
            jobId=job_id,
            range='',
            vaultName=config['glacier']['VaultName'],
        )

    except ClientError as e:
        logging.error(e)
        print("Fail to download_restored_file!")
        return None
    return response

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html


def s3_upload_files(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    my_config = s3_config()
    s3_client = boto3.client('s3', config=my_config)
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def dynamo_query_job(table_name, job_id):
    my_config = s3_config()
    try:
        dynamo = boto3.resource('dynamodb', config=my_config)
        table = dynamo.Table(table_name)
        response = table.query(
            KeyConditionExpression=Key('job_id').eq(job_id)
        )
    except ClientError:
        print("Fail to query items given the job_id!")
        return None
    return response['Items']


def delete_archive(vaultName, archiveId):
    my_config = s3_config()
    try:
        client = boto3.client('glacier', config=my_config)
        response = client.delete_archive(
            accountId='-',
            archiveId=archiveId,
            vaultName=vaultName,
        )
    except ClientError as e:
        print("Fail to delete archive in glacier!")
        logging.error(e)
        return False
    return True


def dynamo_delete_archiveId(table_name, job_id):
    my_config = s3_config()
    try:
        dynamo = boto3.resource('dynamodb', config=my_config)
        table = dynamo.Table(table_name)
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression="SET archived=:r REMOVE results_file_archive_id",
            # no longer archived
            ExpressionAttributeValues={':r': 0},
        )

    except ClientError:
        print("Fail to delete archiveID into dynamo database")
        return False
    return True


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
        print(message)
        job_id = message['JobId']
        archiveId = message['ArchiveId']
        dynamo_job_id = message['JobDescription']

        # download restored file
        response = download_restored_file(job_id)
        print(response)
        # turn streamingbody object to binary string
        content = response['body'].read()

        # upload to s3 result bucket
        job = dynamo_query_job(config['dynamodb']['TableName'], dynamo_job_id)
        result_path = job['s3_key_result_file']
        # result_path = "xuhanxie/44056502-a240-461b-b309-52298b9f6b93/f42e1fb6-2f7c-4d45-a240-c766264577d2~test.annot.vcf"
        with open('temp.vcf', 'wb') as f:
            f.write(content)

        # remove file to s3
        s3_upload_files(
            'temp.vcf', config['s3']['AWS_S3_RESULTS_BUCKET'], result_path)

        # delete temp.vcf
        if os.path.exists('temp.vcf'):
            os.remove('temp.vcf')

        # delete archive in glacier
        delete_archive(config['glacier']['VaultName'], archiveId)

        # delete key 'results_file_archive_id' in dynamodb
        dynamo_delete_archiveId(config['dynamodb']['TableName'], dynamo_job_id)

        # delete message in queue
        sqs_delete_messages(sqs, receipt_handle)


if __name__ == "__main__":
    main()
# EOF
