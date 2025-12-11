import os
import boto3
import json
from chalice import Chalice
from botocore.exceptions import ClientError

app = Chalice(app_name='s3-events')
app.debug = True

# Set the values in the .chalice/config.json file.
S3_BUCKET = os.environ.get('APP_BUCKET_NAME', '')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', '')

<<<<<<< HEAD
#triggers when json files are uploaded to S3
@app.on_s3_event(bucket=S3_BUCKET, events=['s3:ObjectCreated:*'], suffix='.json')
def s3_handler(event):
    # get the event, pull the file from s3, read it, and insert into DDB
    #logs event
    app.log.debug(f"Received bucket event: {event.bucket}, key: {event.key}")
    #reads file from s3, deserializes/ parses json
    data = get_s3_object(event.bucket, event.key)
    #inserts data into table in dynamodb
    insert_data_into_dynamodb(data)
    #returns the data
=======

@app.on_s3_event(bucket=S3_BUCKET, events=['s3:ObjectCreated:*'], suffix='.json')
def s3_handler(event):
    app.log.debug(f"Received bucket event: {event.bucket}, key: {event.key}")
    data = get_s3_object(event.bucket, event.key)
    insert_data_into_dynamodb(data)
>>>>>>> 7f029cd45208f7c437918a08a366ee553f4eaf6f
    return data

#get object from s3
def get_s3_object(bucket, key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
<<<<<<< HEAD
    except ClientError as e:
        app.log.error(f"Error fetching object from S3: {e}")
        raise e
=======
    except Exception as e:
        print(e)
>>>>>>> 7f029cd45208f7c437918a08a366ee553f4eaf6f

def insert_data_into_dynamodb(data):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    try:
        response = table.put_item(
            Item={
                'event_key': data['event_key'],
                'building_code': data['building_code'],
                'building_door_id': data['building_door_id'],
                'access_time': data['access_time'],
                'user_identity': data['user_identity']
            }
        )
        app.log.debug(f"DynamoDB response: {response}")
        return response
    except Exception as e:
        app.log.error(f"Error inserting data into DynamoDB: {e}")
        raise e

#route: deployment
@app.route('/access', methods=['GET'])
def get_access():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    try:
        items = table.scan()['Items']
<<<<<<< HEAD
        #different lambda from aws - one line function to sort by access time
=======
>>>>>>> 7f029cd45208f7c437918a08a366ee553f4eaf6f
        sorted_items = sorted(items, key=lambda x: x['access_time'])
        return sorted_items
    except ClientError as e:
        app.log.error(f"Error scanning DynamoDB table: {e}")
        raise e
<<<<<<< HEAD
   
    
=======
>>>>>>> 7f029cd45208f7c437918a08a366ee553f4eaf6f
