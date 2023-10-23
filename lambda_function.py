import json
import psycopg2
import os
import time 

print('Loading function')
 # Get environment variables
DB_SERVER = os.getenv('DB_SERVER')
DB_PORT= os.environ.get('DB_PORT')
DB_NAME= os.environ.get('DB_NAME')
DB_PASSWORD= os.environ.get('DB_PASSWORD')
DB_USERNAME = os.environ.get('DB_USERNAME')

connection = psycopg2.connect(host=DB_SERVER, port=DB_PORT,dbname=DB_NAME, user=DB_USERNAME, password=DB_PASSWORD)
connection.set_session(autocommit=True)

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    print(event)
    print(f"{DB_SERVER};{DB_PORT}")

    #Loops through every file uploaded
    for record in event['Records']:
        jsonbody=(record["body"])
        notificationMsg=json.loads(jsonbody)
        status_update=json.loads(notificationMsg["Message"])
        print(status_update)
        process_status_update(status_update)

    return {
        'statusCode': 200,
        'body': 'processed successfully'
    }

def process_status_update(status_msg):
    username = status_msg["accountUuid"]
    status = status_msg["seekingJob"]
    update_time_ms = time.ctime(status_msg["sendTimestamp"]/1000)

    update_job_app_status(username, status, update_time_ms)
   

def update_job_app_status(username, status, update_time):
      with connection.cursor() as cursor:
        cursor.execute("""
        UPDATE sjmsjob.job_application
        SET seeker_status = %(status)s, seeker_status_last_updated_date=%(update_time)s
        WHERE user_id = %(username)s AND (seeker_status_last_updated_date IS NULL OR seeker_status_last_updated_date < %(update_time)s)""", {
           'username': username,
           'status': status,
           'update_time': update_time
        });
      connection.commit()
      print("updated db")

