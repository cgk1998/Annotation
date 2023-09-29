# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import logging
import sys
import time
import driver
import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from flask import jsonify, json
import configparser

# get ann configuration
config = configparser.ConfigParser()
config.read('/home/ec2-user/mpcs-cc/gas/ann/ann_config.ini')


"""A rudimentary timer for coarse-grained profiling
"""


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


"""
helper functions for routers
"""


def s3_config():
    my_config = Config(
        region_name='us-east-1',
        signature_version='s3v4',
    )

    return my_config


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
        # response = s3_client.upload_file(file_name, bucket, object_name)
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html


def dynamo_update(table_name, job_id, s3_res_bucket, s3_res, s3_log, completion_time, job_status):
    my_config = s3_config()
    try:
        dynamo = boto3.resource('dynamodb', config=my_config)
        table = dynamo.Table(table_name)
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression="SET job_status=:r, s3_results_bucket=:a, s3_key_result_file=:b, s3_key_log_file=:c, complete_time=:d",
            ExpressionAttributeValues={
                ':r': job_status,
                ':a': s3_res_bucket,
                ':b': s3_res,
                ':c': s3_log,
                ':d': completion_time
            },
        )
    except ClientError:
        print("Fail to update and add info into dynamo database")
        return False

    return True


def notify_complete(data):
    # select the Topic arn to publish
    topic_arn = config['sns']['TopicArnResult']
    data = json.dumps(data)
    print("send email!")

    try:
        my_config = s3_config()
        client = boto3.client('sns', config=my_config)
        client.publish(TopicArn=topic_arn, Message=data)
    except ClientError:
        print("Fail to publish to sns's results topic!")
        return False
    return True


if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')

        data_path = config['ann']['DataPath']
        file_path = sys.argv[1]
        job_id = sys.argv[2]
        email = sys.argv[3]

        file_path_list = sys.argv[1].split('/')
        filename = file_path_list[-1]
        username = file_path_list[-2]

        # remove .vcf suffix
        filename_prefix = filename[:-4]
        log_path = data_path + username + "/" + filename_prefix + ".vcf.count.log"
        annot_path = data_path + username + "/" + filename_prefix + ".annot.vcf"

        # create path in s3
        object_name_prefix = config['s3']['User'] + "/" + username + "/"
        log_obj_name = object_name_prefix + filename_prefix + ".vcf.count.log"
        annot_obj_name = object_name_prefix + filename_prefix + ".annot.vcf"

        bucket_name = config['s3']['BucketResult']
        table_name = config['dynamodb']['TableName']
        completion_time = int(time.time())
        job_status = "COMPLETED"

        # 1. upload the results file
        if not s3_upload_files(annot_path, bucket_name, annot_obj_name):
            print("fail to upload annot file")

        # 2. upload the log file
        if not s3_upload_files(log_path, bucket_name, log_obj_name):
            print("fail to upload log file")

        # 3. update the dynamo database
        if not dynamo_update(table_name, job_id, bucket_name, annot_obj_name, log_obj_name, completion_time, job_status):
            print("fail to update the dynamo database")

        # 4. publishes a notification to sns and trigger lambda function
        data = {
            "job_id": job_id,
            "email": email,
            "completion_time": completion_time,
        }
        print(data)

        if not notify_complete(data):
            print("fail to notify the job completion to users!")

        # 5. clean up local job files
        if os.path.exists(annot_path):
            print(annot_path)
            print('remove annot path')
            os.remove(annot_path)
        if os.path.exists(log_path):
            print(log_path)
            print('remove log path')
            os.remove(log_path)
        if os.path.exists(file_path):
            print(file_path)
            print('remove user path')
            os.remove(file_path)

    else:
        print("A valid .vcf file must be provided as input to this program.")
