import json

print('Loading function')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    print(event)
    
    return event['key1']  # Echo back the first key value
    #raise Exception('Something went wrong')
