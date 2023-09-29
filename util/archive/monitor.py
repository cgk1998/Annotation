from configparser import ConfigParser
from boto3.dynamodb.conditions import Key
import time
import json
import os
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import logging
from decimal import Decimal
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


def dynamo_scan(TableName):
    my_config = s3_config()
    try:
        dynamo = boto3.resource('dynamodb', config=my_config)
        table = dynamo.Table(TableName)
        response = table.scan()
    except ClientError as e:
        logging.error(e)
        print("Fail to retrieve scan result from dynamo db!")
        return None
    return response['Items']


# solve the decimal serialize problem
def default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Object of type '%s' is not JSON serializable" %
                    type(obj).__name__)


def dynamo_update_archive(table_name, job_id, archiveId):
    my_config = s3_config()
    try:
        dynamo = boto3.resource('dynamodb', config=my_config)
        table = dynamo.Table(table_name)
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression="SET results_file_archive_id=:r, archived=:p",
            ExpressionAttributeValues={
                ':r': archiveId,
                # 1 means true, file archived
                ':p': 1
            },
        )
    except ClientError:
        print("Fail to update archiveID into dynamo database")
        return False
    return True


def sns_pub(data):
    # select the Topic arn to publish
    topic_arn = config['sns']['TopicArnArchive']
    data = json.dumps(data, default=default)

    try:
        my_config = s3_config()
        client = boto3.client('sns', config=my_config)
        client.publish(TopicArn=topic_arn, Message=data)
    except ClientError:
        print("fail to publish to sns archive")
        return False
    return True


def main():
    while True:
        time.sleep(10)
        items = dynamo_scan(config['dynamodb']['TableName'])
        for item in items:
            # check free_user with completed job status
            if 'results_file_archive_id' not in item.keys() and 'user_role' in item.keys() and item['user_role'] == 'free_user' and item['job_status'] == 'COMPLETED':
                complete_time = int(item['complete_time'])
                current_time = int(time.time())
                time_lapsed = current_time - complete_time
                # time_lapsed exceeds 5 minutes
                if time_lapsed > int(config['free_user']['FreeTime']):
                    # public to sns for archive
                    job_id = item['job_id']
                    # avoid to be scanned repeatedly
                    dynamo_update_archive(
                        config['dynamodb']['TableName'], job_id, 0)
                    sns_pub(item)


if __name__ == "__main__":
    main()
