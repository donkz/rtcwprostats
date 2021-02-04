import logging
import boto3
import json
from reader_writeddb import (
    ddb_put_item,
    ddb_prepare_match_item,
    ddb_prepare_stats_items,
    ddb_prepare_statsall_item,
    ddb_prepare_gamelog_item,
    ddb_prepare_wstat_items,
    ddb_prepare_wstatsall_item,
    ddb_prepare_player_items
    )


# pip install --target ./ sqlalchemy
# import sqlalchemy

logger = logging.getLogger()
logger.setLevel(logging.INFO)  # set to DEBUG for verbose boto output
logging.basicConfig(level = logging.INFO)

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('rtcwprostats')
s3 = boto3.client('s3')

def handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    logger.info('Reading {} from {}'.format(file_key, bucket_name))
        
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    except s3.exceptions.ClientError as err:
        if err.response['Error']['Code'] == 'EndpointConnectionError':
            print("[x] Connection could not be established to AWS. Possible firewall or proxy issue. " + str(err))
        elif err.response['Error']['Code'] == 'ExpiredToken':
            print("[x] Credentials for AWS S3 are not valid. " + str(err))
        elif err.response['Error']['Code'] == 'AccessDenied':
            print("[x] Current credentials to not provide access to read the file. " + str(err))
        elif err.response['Error']['Code'] == 'NoSuchKey':
            print("[x] File was not found: " + file_key)
        else:
            print("[x] Unexpected error: %s" % err)
        return None

    message = "Nothing was processed"
    try:
        content = obj['Body'].read().decode('UTF-8')
        gamestats = json.loads(content)
        logger.info("Number of keys in the file: " + str(len(gamestats.keys())))
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        # Todo expand these
        message = "Failed to read content from " + file_key + "\n" + error_msg
    
    items = []
    items.append(ddb_prepare_match_item(gamestats))
    items.extend(ddb_prepare_stats_items(gamestats))
    items.append(ddb_prepare_statsall_item(gamestats))
    items.append(ddb_prepare_gamelog_item(gamestats))
    items.extend(ddb_prepare_wstat_items(gamestats))
    items.append(ddb_prepare_wstatsall_item(gamestats))
    items.extend(ddb_prepare_player_items(gamestats))

    message = "Successfully sent " + file_key + " to database" 
    for Item in items:
        response = None
        try:
            response = ddb_put_item(Item, table)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            message = "Failed to load all records for a match " + file_key + "\n" + error_msg
         
        if response["ResponseMetadata"]['HTTPStatusCode'] != 200:
            print(response)
            message = "Item returned non200 code " + Item["pk"] + ":" + Item["sk"]

    return {
        'message': message
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
              "key": "intake/gameStats_match_1610684722_round_2_mp_base.json",
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
