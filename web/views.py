# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
import logging
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, jsonify,
                   request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


'''
helper functions for router
'''


def s3_config():
    my_config = Config(
        region_name=app.config['AWS_REGION_NAME'],
        signature_version=app.config['AWS_SIGNATURE_VER'],
    )
    return my_config


def get_s3_files(dir_path):
    try:
        my_config = s3_config()
        s3 = boto3.resource('s3', config=my_config)
        bucket = s3.Bucket(app.config['AWS_S3_INPUTS_BUCKET'])
        file_dir = bucket.objects.filter(Delimiter='/', Prefix=dir_path)
    except ClientError:
        print(ClientError)
        return
    file_list = []
    for obj in file_dir:
        file_list.append(obj.key)
    file_list.remove(dir_path)
    return file_list


def dynamo_write(table_name, data):
    '''
    write data into dynamo database
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
    '''
    my_config = s3_config()

    try:
        dynamo = boto3.resource('dynamodb', config=my_config)
        table = dynamo.Table(table_name)
        table.put_item(Item=data)

    except ClientError:
        print("Fail to write data into dynamo database")
        return False

    return True


def dynamo_query(table_name):
    # Reference: Canvas modules class5 dynamo_reader.py
    my_config = s3_config()
    try:
        dynamo = boto3.resource('dynamodb', config=my_config)
        table = dynamo.Table(table_name)
        response = table.query(
            IndexName='user_id_index',
            KeyConditionExpression=Key('user_id').eq(
                session['primary_identity'])
        )
    except ClientError:
        app.logger.error("Fail to query items from dynamo database!")
        return None
    return response['Items']


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


def sns_pub_ann(data):
    # select the Topic arn to publish
    topic_arn = app.config['AWS_SNS_JOB_REQUEST_TOPIC']
    data = json.dumps(data)

    try:
        my_config = s3_config()
        client = boto3.client('sns', config=my_config)
        client.publish(TopicArn=topic_arn, Message=data)
    except ClientError:
        print("Fail to publish to sns request topic!")
        return False
    return True


def sns_pub_restore(data):
    # select the Topic arn to public
    topic_arn = app.config['AWS_SNS_RESTORE_TOPIC']
    data = json.dumps(data)

    try:
        my_config = s3_config()
        client = boto3.client('sns', config=my_config)
        client.publish(TopicArn=topic_arn, Message=data)
    except ClientError as e:
        logging.error(e)
        print("Fail to publish to sns restore topic!")
        return False
    return True
# https://www.programiz.com/python-programming/datetime/timestamp-datetime


def time_conversion(timestamp):
    dt_object = datetime.fromtimestamp(timestamp)
    time_required = dt_object.strftime("%Y-%m-%d %H:%M")
    return time_required

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html


def s3_download_url(bucket_name, object_name):
    my_config = s3_config()
    s3_client = boto3.client('s3', config=my_config)
    try:
        # Generate a presigned URL for the S3 object
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=600)
    except ClientError as e:
        print("Fail to get the presigned downloading url of s3!")
        logging.error(e)
        return None
    # The response contains the presigned URL
    return response

# https://stackoverflow.com/questions/47558588/boto3-read-a-file-content-from-s3-key-line-by-line


def read_s3_file(bucket_name, object_name):
    my_config = s3_config()

    try:
        s3 = boto3.resource('s3', config=my_config)
        content = s3.Object(bucket_name, object_name).get()[
            'Body'].read().decode('utf-8')
        print(content)
    except ClientError as e:
        print("Fail to read log file in s3!")
        logging(e)
        return None
    return content


def upgrade_to_premium(table_name, items):
    my_config = s3_config()
    for item in items:
        try:
            dynamo = boto3.resource('dynamodb', config=my_config)
            table = dynamo.Table(table_name)
            table.update_item(
                Key={'job_id': item['job_id']},
                UpdateExpression="SET user_role=:r",
                ExpressionAttributeValues={
                    ':r': 'premium_user'
                },
            )
        except ClientError as e:
            logging.error(e)
            print("Fail to upgrade user to premium into dynamo database")
            return False

    return True


def cancel_to_free(table_name, items):
    my_config = s3_config()
    for item in items:
        try:
            dynamo = boto3.resource('dynamodb', config=my_config)
            table = dynamo.Table(table_name)
            table.update_item(
                Key={'job_id': item['job_id']},
                UpdateExpression="SET user_role=:r",
                ExpressionAttributeValues={
                    ':r': 'free_user'
                },
            )
        except ClientError as e:
            logging.error(e)
            print("Fail to cancel user to free into dynamo database")
            return False

    return True


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


# identify the user_role to decide whether permit the upload
@app.route('/validate', methods=['GET'])
def validate():
    profile = get_profile(identity_id=session.get('primary_identity'))
    user_role = profile.role
    print(user_role)
    if user_role == 'premium_user':
        return jsonify({'status': True})
    return jsonify({'status': False})


@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
    # Create a session client to the S3 service
    s3 = boto3.client('s3',
                      region_name=app.config['AWS_REGION_NAME'],
                      config=Config(signature_version='s3v4'))

    bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
    user_id = session['primary_identity']

    # Generate unique ID to be used as S3 key (name)
    key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
        str(uuid.uuid4()) + '~${filename}'

    # Create the redirect URL
    redirect_url = str(request.url) + '/job'

    # Define policy fields/conditions
    encryption = app.config['AWS_S3_ENCRYPTION']
    acl = app.config['AWS_S3_ACL']
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl}
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

    # Get bucket name, key, and job ID from the S3 redirect URL
    bucket_name = str(request.args.get('bucket'))
    s3_key = str(request.args.get('key'))

    # Extract the job ID from the S3 key
    object_name = s3_key.replace("%2F", "/")
    print(object_name)
    job_id = object_name.split("/")[2].split("~")[0]
    print(job_id)
    user_id = session['primary_identity']
    print(user_id)
    profile = get_profile(identity_id=session.get('primary_identity'))
    user_role = profile.role
    print(user_role)
    user_email = profile.email
    # Create a job item and persist it to the annotations database
    # https://www.runoob.com/python/att-time-time.html
    data = {
        "job_id": job_id,
        "user_id": user_id,
        "input_file_name": object_name.split('~')[1],
        "s3_inputs_bucket": bucket_name,
        "s3_key_input_file": object_name,
        "submit_time": int(time.time()),
        "job_status": "PENDING",
        "user_role": user_role,
        "email": user_email,
    }
    # Persist job to database
    # Move your code here...
    # check whether the dynamo database have the table or not
    if not dynamo_write(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'], data):
        app.logger.error("Fail to write data into dynamo database!")
        return abort(500)

    # Send message to request queue
    # Move your code here...
    if not sns_pub_ann(data):
        app.logger.error("Fail to public request topic to sns!")
        return abort(500)

    return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""


@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():

    # Get list of annotations to display
    table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
    item_list = dynamo_query(table_name)
    if item_list is None:
        print("No data retrieved from dynamodb!")
        return abort(500)
    for item in item_list:
        item['submit_time'] = time_conversion(int(item['submit_time']))

    return render_template('annotations.html', annotations=item_list)


"""Display details of a specific annotation job
"""


@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    job_detail = dynamo_query_job(
        app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'], id)
    if job_detail is None:
        print("Fail to retrieve the detail of this job!")
        return abort(500)
    job_detail = job_detail[0]

    # check user authority
    username = session['primary_identity']
    if username != job_detail['user_id']:
        print('Not authorized to view this job”.')
        return abort(500)

    # follow the pipeline in annotation_details.html
    s3_input_download_url = s3_download_url(
        app.config['AWS_S3_INPUTS_BUCKET'], job_detail['s3_key_input_file'])
    free_access_expired = False
    # if job is completed
    if job_detail['job_status'] == 'COMPLETED':
        # time format conversion
        job_detail['complete_time'] = time_conversion(
            int(job_detail['complete_time']))
        if job_detail['user_role'] == 'free_user' and 'results_file_archive_id' in job_detail.keys():
            free_access_expired = True

        # check archived status, 0 means
        print(job_detail)
        if job_detail['user_role'] == 'premium_user':
            print("This is a premium_user")
            if 'archived' in job_detail.keys():
                archived = job_detail['archived']
                # premium_user but the file is restoring
                if archived == 1:
                    print("archived = 1")
                    job_detail['restore_message'] = "You files are restoring!"

        s3_ann_download_url = s3_download_url(
            app.config['AWS_S3_RESULTS_BUCKET'], job_detail['s3_key_result_file'])
    else:
        # time format conversion
        s3_ann_download_url = ""
        job_detail['submit_time'] = time_conversion(
            int(job_detail['submit_time']))

    s3_log_url = request.url_root + '/annotations/' + id + '/log'
    job_detail['input_file_url'] = s3_input_download_url
    job_detail['ann_file_url'] = s3_ann_download_url
    job_detail['log_file_url'] = s3_log_url

    return render_template('annotation_details.html', annotation=job_detail, free_access_expired=free_access_expired)


"""Display the log file contents for an annotation job
"""


@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    # query the job given the id
    job_detail = dynamo_query_job(
        app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'], id)
    if job_detail is None:
        print("Fail to retrieve the detail of this job!")
        return abort(500)
    job_detail = job_detail[0]

    # check job status
    if job_detail['job_status'] != "COMPLETED":
        print("The job is not completed yet!")
        return abort(500)

    # read content of the log file
    content = read_s3_file(
        app.config['AWS_S3_RESULTS_BUCKET'], job_detail['s3_key_log_file'])
    if content is None:
        print("The log file for the given job_id is empty!")
        return abort(500)

    return render_template('view_log.html', log_file_contents=content, job_id=id)


"""Subscription management handler
"""


@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
    if (request.method == 'GET'):
        # Display form to get subscriber credit card info
        if (session.get('role') == "free_user"):
            return render_template('subscribe.html')
        else:
            return redirect(url_for('profile'))

    elif (request.method == 'POST'):
        # Update user role to allow access to paid features
        update_profile(
            identity_id=session['primary_identity'],
            role="premium_user"
        )

        # Update role in the session
        session['role'] = "premium_user"

        # update the role in the dynamodb
        user_items = dynamo_query(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
        upgrade_to_premium(
            app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'], user_items)

        data = {
            "user_id": session['primary_identity'],
        }
        # user becomes premium
        # notify to restore the archived file of free_user
        sns_pub_restore(data)

        # Request restoration of the user's data from Glacier
        # Add code here to initiate restoration of archived user data
        # Make sure you handle files not yet archived!

        # Display confirmation page
        return render_template('subscribe_confirm.html')


"""Reset subscription
"""


@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(
        identity_id=session['primary_identity'],
        role="free_user"
    )
    session['role'] = "free_user"
    # update the role in the dynamodb
    user_items = dynamo_query(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    cancel_to_free(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'], user_items)

    return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""


@app.route('/', methods=['GET'])
def home():
    return render_template('home.html')


"""Login page; send user to Globus Auth
"""


@app.route('/login', methods=['GET'])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if (request.args.get('next')):
        session['next'] = request.args.get('next')
    return redirect(url_for('authcallback'))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html',
                           title='Page not found', alert_level='warning',
                           message="The page you tried to reach does not exist. \
      Please check the URL and try again."
                           ), 404


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return render_template('error.html',
                           title='Not authorized', alert_level='danger',
                           message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
                           ), 403


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return render_template('error.html',
                           title='Not allowed', alert_level='warning',
                           message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
                           ), 405


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html',
                           title='Server error', alert_level='danger',
                           message="The server encountered an error and could \
      not process your request."
                           ), 500

# EOF
