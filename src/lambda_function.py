import json
import psycopg2
import os

print('Loading function')
 # Get environment variables
DB_SERVER = os.getenv('DB_SERVER')250
DB_PORT= os.environ.get('DB_PORT')
DB_NAME= os.environ.get('DB_NAME')
DB_PASSWORD= os.environ.get('DB_PASSWORD')
DB_USERNAME = os.environ.get('DB_USERNAME')

connection = psycopg2.connect(host= f"{DB_SERVER};{DB_PORT}",database=DB_NAME, user=DB_USERNAME, password=DB_PASSWORD)
connection.set_session(autocommit=True)

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    print(event)
    print(f"{DB_SERVER};{DB_PORT}")

    #Loops through every file uploaded
    for record in event['Records']:
        jsonbody=(record["body"])
        status_update=json.loads(jsonbody)

    return {
        'statusCode': 200,
        'body': 'processed successfully'
    }

def process_status_update(status_msg):
    username = status_msg.accountUuid
    status = status_msg.seekingJob
    update_time = status_msg.sendTime
    update_job_app_status(username, status, update_time)
   

def update_job_app_status(username, status, update_time):
      connection.execute("""
        UPDATE sjmsjob.job_application
        SET seeker_status = status, seeker_status_last_updated_date = update_time
        WHERE user_id = username and seeker_status_last_updated_date < update_time""", {
           'username': username,
           'status': status,
           'update_time': update_time
        });
