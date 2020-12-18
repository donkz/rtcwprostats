import json
import pprint
import base64
import logging
import datetime
import boto3
import os, sys
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    display_event = False
    long_response = False
    
    header_name = "matchid"
    
    #defaults
    time_string = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    
    grief = False
    folder = "intake/"
    file_suffix = ".txt"
    matchid = "default_match_id"
    
    try:
        matchid = event["headers"][header_name] # id:1234-4567
        if len(matchid) == 0:
            matchid = "empty-header"
    except:
        grief = True
        logger.warning("Could not extract file name from headers: " + str(event["headers"]))
    
    body = event['body']
    try:
        body = str(base64.b64decode(event['body']),"utf-8")
    except:
        grief = True
        logger.warning("Could not decode body: " + str(event["body"]))
    
    folder = "intakefail/" if grief else "intake/"
    file_name = folder + time_string + "-" + matchid + file_suffix

    if long_response:
        response = "I will save your payload\n" + body + " \nto file " + file_name
    
    try:
        s3 = boto3.client("s3")
        s3.put_object(Bucket="rtcwprostats", Key=file_name, Body=body)
        response = "UPLOADED " + file_name
    except:
        response = "FAILED " + file_name
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=5, file=sys.stdout)
        
    if display_event:
        response = pprint.pformat(event, indent=4)
        
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': response
    }