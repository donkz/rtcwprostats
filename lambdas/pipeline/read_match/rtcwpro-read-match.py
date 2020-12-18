import logging
import boto3
import json
#pip install --target ./ sqlalchemy
import sqlalchemy

logger = logging.getLogger()
logger.setLevel(logging.INFO) #set to DEBUG for verbose boto output
#logging.basicConfig(level = logging.INFO)


def handler(event, context):
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    logger.info('Reading {} from {}'.format(file_key, bucket_name))
    logger.info(sqlalchemy.__version__)

    #debug = True

    s3 = boto3.client('s3')
    #bucket = s3.Bucket('rtcwprostats')
    
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    except s3.exceptions.ClientError as err:
        if err.response['Error']['Code'] == 'EndpointConnectionError':
            print("[x] Connection could not be established to AWS. Possible firewall or proxy issue. " + str(err))
        elif err.response['Error']['Code'] == 'ExpiredToken':
            print("[x] Credentials for AWS S3 are not valid. " + str(err))
        elif err.response['Error']['Code'] == 'AccessDenied':
            print("[x] Current credentials to not provide access to read the file. " + str(err))
        else:
            print("[x] Unexpected error: %s" % err)
        return None
        
    content = obj['Body'].read().decode('cp1252')
    json_ = json.loads(content)
    print("[ ] Number of keys in the file: " + str(len(json_.keys())))
    
    
    message ="Nothing was processed"
    if True:
        message = "Something was processed"
        
    return { 
        'message' : message
    } 
    
if __name__ == "__main__":
    event_str = """
    {
      "Records": [
        {
          "eventVersion": "2.0",
          "eventSource": "aws:s3",
          "awsRegion": "us-east-1",
          "eventTime": "1970-01-01T00:00:00.000Z",
          "eventName": "ObjectCreated:Put",
          "userIdentity": {
            "principalId": "EXAMPLE"
          },
          "requestParameters": {
            "sourceIPAddress": "127.0.0.1"
          },
          "responseElements": {
            "x-amz-request-id": "EXAMPLE123456789",
            "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
          },
          "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "testConfigRule",
            "bucket": {
              "name": "rtcwprostats",
              "ownerIdentity": {
                "principalId": "EXAMPLE"
              },
              "arn": "arn:aws:s3:::example-bucket"
            },
            "object": {
              "key": "intake/20201127-204300-good-api-test.txt",
              "size": 1024,
              "eTag": "0123456789abcdef0123456789abcdef",
              "sequencer": "0A1B2C3D4E5F678901"
            }
          }
        }
      ]
    }
    """
    event = json.loads(event_str)
    print(handler(event, None))