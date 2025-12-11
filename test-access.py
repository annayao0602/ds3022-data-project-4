import boto3
import random
import string
import time
import datetime
import json
import os

BUCKET_NAME = 'zzz2bx-access'

def generate_test_event():
    # event_key is an epoch timestamp for now. Cast as string.
    event_key = str(int(time.time()))

    # building_code is a random selection
    buildings = ['A01', 'B02', 'C03', 'D04', 'E05', 'F06', 'G07', 'H08', 'I09', 'J10']
    building_code = random.choice(buildings)

    # building_door_id is a random string of 2 digits
    building_door_id = str(random.choice(range(10, 70)))

    # access_time is a current date time stamp
    access_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # user_identity is in the UVA ID format
    users = ["alpha1b", "beta2c", "gamma3d", "delta4e", "epsilon5f", "zeta6g", "eta7h", "theta8i", "iota9j"]
    user_identity = random.choice(users)

    # write these as a json file and upload to the s3 bucket
    data = {
        'event_key': event_key,
        'building_code': building_code,
        'building_door_id': building_door_id,
        'access_time': access_time,
        'user_identity': user_identity
    }
    try:
        with open('test-event.json', 'w') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error writing to file: {e}")
        return None

    try:
        s3 = boto3.client('s3')
        s3.upload_file('test-event.json', BUCKET_NAME, f'test-event-{event_key}.json')
        #then delete the file
        os.remove('test-event.json')
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return None
    return data

if __name__ == '__main__':
    generate_test_event()
