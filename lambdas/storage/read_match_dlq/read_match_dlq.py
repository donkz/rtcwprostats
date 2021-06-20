import logging
import boto3
import json
import time as _time
import os

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('readdlq')
logger.setLevel(log_level)

s3 = boto3.resource('s3')

def handler(event, context):
    """Read new incoming json and submit it to the DB."""
    
    # sqs event source
    s3_request_from_sqs = event['Records'][0]
    bucket_name = s3_request_from_sqs['s3']['bucket']['name']
    file_key    = s3_request_from_sqs['s3']['object']['key']
    dlq_key = file_key.replace("intake","reader_dlq")

    logger.info('Copying {} to read_dlq'.format(file_key))

    copy_source = {
        'Bucket': bucket_name,
        'Key': file_key
    }
    bucket = s3.Bucket(bucket_name)
    new_obj = bucket.Object(dlq_key)
    new_obj.copy(copy_source)
    
    logger.info('Deleting object {}'.format(file_key))
    s3.Object(bucket_name, file_key).delete()
    
    logger.info('Done')
    

if __name__ == "__main__":
    event_str = """
    {
      "Records": [
        {
          "eventVersion": "2.1",
          "eventSource": "aws:s3",
          "awsRegion": "us-east-1",
          "eventTime": "2021-06-17T20:51:14.045Z",
          "eventName": "ObjectCreated:Put",
          "userIdentity": {
            "principalId": "AWS:AROA3RJVP5VAO5OYH3DA3:rtcwpro-save-payload"
          },
          "requestParameters": {
            "sourceIPAddress": "3.237.39.109"
          },
          "responseElements": {
            "x-amz-request-id": "9YF9X4WFV4BN7GDS",
            "x-amz-id-2": "wLZ7cl/+yqFbimmdzSMfbBvUH0M15yfImDQeSJUrDIRo8LK39iS17XRMEKLX3An6GaTYWYgd43DCUszV0SFd7uANgaUEKrh4"
          },
          "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "NWZjMDJhNmItOWVkNi00ZDg2LWE3MWQtZjc0ZDk2MTE2ZmI2",
            "bucket": {
              "name": "rtcwprostats",
              "ownerIdentity": {
                "principalId": "A2QI3L1FD36UKG"
              },
              "arn": "arn:aws:s3:::rtcwprostats"
            },
            "object": {
              "key": "intake/20210617-221634-1610335502.txt",
              "size": 83217,
              "eTag": "449d81dfa9a2972f7403e7ab829f0c96",
              "versionId": "2x1l_syjDGIbAPTWmRhjNjp.MJ1vA.cK",
              "sequencer": "0060CBB5CCE09BCAB5"
            }
          }
        }
      ]
    }
    """
    event = json.loads(event_str)
    handler(event, None)
    
    



