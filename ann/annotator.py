import uuid
import shutil
import subprocess
import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import json
import configparser

# get ann configuration
config = configparser.ConfigParser()
config.read('/home/ec2-user/mpcs-cc/gas/ann/ann_config.ini')

'''
helper function
'''

# global variable
queue_url = config['sqs']['sqsRequestsUrl']


def s3_config():
    my_config = Config(
        region_name=config['aws']['AwsRegionName'],
        signature_version=config['aws']['SignatureVersion'],
    )

    return my_config


def s3_download_files(bucket_name, object_name, file_name):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
    try:
        my_config = s3_config()
        s3 = boto3.client('s3', config=my_config)
        with open(file_name, 'wb') as f:
            s3.download_fileobj(bucket_name, object_name, f)
    except ClientError:
        return False
    return True


def submit_job(file_path, job_id, file_name, email):
    # try to handle the error or launching the annotator
    try:
        subprocess.Popen(
            'python {} {} {} {}'.format(config['ann']['AnnRunPath'], file_path, job_id, email), shell=True)
    except:
        return json.dumps({'code': 500, 'status': 'error', 'message': 'fail to launch the annotator'})

    return json.dumps({"code": 201, "data": {"job_id": job_id, "input_file": str(file_name)}})


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
def dynamo_update(table_name, job_id, state):
    my_config = s3_config()
    try:
        dynamo = boto3.resource('dynamodb', config=my_config)
        table = dynamo.Table(table_name)
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression="SET job_status=:r",
            # only when pending to running
            ConditionExpression="job_status=:p",
            ExpressionAttributeValues={
                ':r': state,
                ':p': 'PENDING',
            },
        )
    except ClientError:
        print("Fail to update job_status in dynamo database")
        return False

    return True


def sqs_poll_message(sqs):

    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-long-polling.html#enable-long-polling-on-message-receipt
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
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


def sqs_delete_messages(sqs, receipt_handle):
    try:
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
    except ClientError:
        print("fail to delete message in sqs")
        return False
    return True


def annotation(data):
    # Extract job parameters from the request body
    bucket_name = data["s3_inputs_bucket"]
    object_name = data["s3_key_input_file"]
    job_id = data['job_id']
    email = data['email']

    if bucket_name is None or object_name is None:
        return json.dumps({'code': 400, 'status': 'error', 'message': 'no correct url parameters generated'}), False

    # Get the input file s3 object and copy it to a local file
    data_path = config['ann']['DataPath']
    user_file = object_name.split('/')
    username = user_file[1]
    filename = user_file[-1]

    user_path = data_path + username
    if not os.path.isdir(user_path):
        os.makedirs(user_path)

    # download the input file from s3 to instance
    file_path = user_path + '/' + filename
    if not s3_download_files(bucket_name, object_name, file_path):
        return json.dumps({'code': 400, 'status': 'error', 'message': 'fail to download files'}), False

    # annotate the job
    print("submit job")
    submit_response = json.loads(submit_job(
        file_path, job_id, object_name, email))

    # update the job_status of dynamo database from "PENDING" to "RUNNING"
    if not dynamo_update(config['dynamodb']['TableName'], job_id, "RUNNING"):
        return json.dumps({'code': 500, 'status': 'error', 'message': 'fail to update the info in dynamo database'}), False

    # fail to launch the annotator
    if submit_response['code'] == 500:
        return json.dumps({'code': 500, 'status': 'error', 'message': 'fail to launch the annotator'}), False

    # successfully submission
    else:
        return json.dumps({'code': 201, 'data': {'id': job_id, 'input_file': object_name}}), True


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
        annotation_res, bool_ann = annotation(message)
        annotation_res = json.loads(annotation_res)
        if not bool_ann:
            print('Fail to successfully complete the annotation process with \ncode: {} \nerror: {}'.format(
                annotation_res['code'], annotation_res['message']))

        # delete message in queue
        if not sqs_delete_messages(sqs, receipt_handle):
            print("Fail to delete messages in sqs")


if __name__ == "__main__":
    main()
