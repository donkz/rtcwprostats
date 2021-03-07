import json
import pprint
import base64
import logging
import datetime
import boto3
import os, sys
import traceback

log_level = logging.INFO
logging.basicConfig(format='%(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(log_level)


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
        matchid = event["headers"][header_name] # id:12344567
        if len(matchid) == 0:
            logger.warning("Bad request header with empty matchid: " + matchid)
            matchid = "empty-header"
            grief = True
        elif not matchid.isnumeric():
            logger.warning("Bad request header value for matchid: " + matchid)
            grief = True
        else:
             logger.info("Received a request with matchid: " + matchid)        
    except:
        grief = True
        logger.warning("Could not extract file name from headers: " + str(event["headers"]))
    
    try:
        submitter_ip = event["headers"]["X-Forwarded-For"]
        if len(submitter_ip) == 0:
            logger.warning("submitter_ip is empty")
            submitter_ip = "empty.ip"
        else:
             logger.info("The request is from : " + submitter_ip)        
    except:
        logger.warning("Could not extract submitter IP from headers: " + str(event["headers"]))
        submitter_ip = "no.ip"
    
    body = event['body']
    try:
        body = str(base64.b64decode(event['body']),"utf-8")
        print("Body first and last", body[1],body[-1])
        if body[0] == "{" and body[-1] == "}":
             ip_insertion = f',"submitter_ip":"{submitter_ip}"'
             body = body[:-1] + ip_insertion + body[-1:]       
    except:
        grief = True
        logger.warning("Could not decode body: " + str(event["body"]))
    
    folder = "intake_dlq/" if grief else "intake/"
    file_name = folder + time_string + "-" + matchid + file_suffix

    if long_response:
        response = "I will save your payload\n" + body + " \nto file " + file_name
    
    try:
        s3 = boto3.client("s3")
        s3.put_object(Bucket="rtcwprostats", Key=file_name, Body=body)
        response = "UPLOADED " + file_name
    except:
        logger.warning("Could not save to file " + str(file_name))
        response = "FAILED " + file_name
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=5, file=sys.stdout)
        
    if display_event:
        response = pprint.pformat(event, indent=4)
        pprint.pformat(event, indent=4)
        
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': response
    }

if __name__ == "__main__":
    event_str = """
    {
     "body" : "ewogICJzZXJ2ZXJpbmZvIjogewogICAgInNlcnZlck5hbWUiOiAiUlRDVyBOQSBQVUIgUlRDV1BSTyBOLlZpcmdpbmlhIiwKICAgICJzZXJ2ZXJJUCI6ICIiLAogICAgImdhbWVWZXJzaW9uIjogIlJ0Y3dQcm8gMS4xLjEiLAogICAgImpzb25HYW1lU3RhdFZlcnNpb24iOiAiMC4xLjIiLAogICAgImdfZ2FtZVN0YXRzbG9nIjogIjE2IiwKICAgICJnX2N1c3RvbUNvbmZpZyI6ICIiLAogICAgImdfZ2FtZXR5cGUiOiAiNiIsCiAgICAidW5peHRpbWUiOiAiMTYxMzQzMDU4NSIKICB9LAogICJnYW1lbG9nIjogWwogICAgewogICAgICAidW5peHRpbWUiOiAiMTYxMzQzMDU4NSIsCiAgICAgICJncm91cCI6ICJzZXJ2ZXIiLAogICAgICAibGFiZWwiOiAicm91bmRfc3RhcnQiCiAgICB9LAogICAgewogICAgICAidW5peHRpbWUiOiAiMTYxMzQzMDcwNCIsCiAgICAgICJncm91cCI6ICJzZXJ2ZXIiLAogICAgICAibGFiZWwiOiAicm91bmRfZW5kIgogICAgfQogIF0sCiAgImdhbWVpbmZvIjogewogICAgIm1hdGNoX2lkIjogIjk5OTk5IiwKICAgICJyb3VuZCI6ICIyIiwKICAgICJyb3VuZF9zdGFydCI6ICIxNjEzNDMwNTg1IiwKICAgICJyb3VuZF9lbmQiOiAiMTYxMzQzMDcwNCIsCiAgICAibWFwIjogInRlX2VzY2FwZTIiLAogICAgInRpbWVfbGltaXQiOiAiMjowMCIsCiAgICAiYWxsaWVzX2N5Y2xlIjogIjIwIiwKICAgICJheGlzX2N5Y2xlIjogIjMwIiwKICAgICJ3aW5uZXIiOiAiICIKICB9LAogICJzdGF0cyI6IFsKICAgIHsKICAgICAgIkYxRTM1N0E1NDM4MEVCIjogewogICAgICAgICJhbGlhcyI6ICJub3RyaXYiLAogICAgICAgICJ0ZWFtIjogIkF4aXMiLAogICAgICAgICJzdGFydF90aW1lIjogMjQ3MjE2MDAsCiAgICAgICAgIm51bV9yb3VuZHMiOiAyLAogICAgICAgICJjYXRlZ29yaWVzIjogewogICAgICAgICAgImtpbGxzIjogMiwKICAgICAgICAgICJkZWF0aHMiOiA1LAogICAgICAgICAgImdpYnMiOiAwLAogICAgICAgICAgInN1aWNpZGVzIjogMSwKICAgICAgICAgICJ0ZWFta2lsbHMiOiAwLAogICAgICAgICAgImhlYWRzaG90cyI6IDMsCiAgICAgICAgICAiZGFtYWdlZ2l2ZW4iOiA3MjMsCiAgICAgICAgICAiZGFtYWdlcmVjZWl2ZWQiOiA2MjIsCiAgICAgICAgICAiZGFtYWdldGVhbSI6IDEzNiwKICAgICAgICAgICJoaXRzIjogNjAsCiAgICAgICAgICAic2hvdHMiOiAxNTksCiAgICAgICAgICAiYWNjdXJhY3kiOiAzNy43MzU4NDkwNTY2MDM3NzYsCiAgICAgICAgICAicmV2aXZlcyI6IDEsCiAgICAgICAgICAiYW1tb2dpdmVuIjogNCwKICAgICAgICAgICJoZWFsdGhnaXZlbiI6IDAsCiAgICAgICAgICAicG9pc29uZWQiOiAwLAogICAgICAgICAgImtuaWZla2lsbHMiOiAwLAogICAgICAgICAgImtpbGxwZWFrIjogMSwKICAgICAgICAgICJlZmZpY2llbmN5IjogMjguMCwKICAgICAgICAgICJzY29yZSI6IDIzLAogICAgICAgICAgImR5bl9wbGFudGVkIjogMiwKICAgICAgICAgICJkeW5fZGVmdXNlZCI6IDAsCiAgICAgICAgICAib2JqX2NhcHR1cmVkIjogMCwKICAgICAgICAgICJvYmpfZGVzdHJveWVkIjogMCwKICAgICAgICAgICJvYmpfcmV0dXJuZWQiOiAyLAogICAgICAgICAgIm9ial90YWtlbiI6IDQKICAgICAgICB9CiAgICAgIH0KICAgIH0KICBdLAogICJ3c3RhdHMiOiBbCiAgICB7CiAgICAgICJGMUUzNTdBNTQzODBFQiI6IFsKICAgICAgICB7CiAgICAgICAgICAid2VhcG9uIjogIk1QLTQwIiwKICAgICAgICAgICJraWxscyI6IDEsCiAgICAgICAgICAiZGVhdGhzIjogMSwKICAgICAgICAgICJoZWFkc2hvdHMiOiAzLAogICAgICAgICAgImhpdHMiOiAyOCwKICAgICAgICAgICJzaG90cyI6IDkxCiAgICAgICAgfQogICAgICBdCiAgICB9CiAgXQp9",
     "headers" : 
         {
             "matchid":"12345",
             "X-Forwarded-For":"192.168.1.0"
             }
     }
    """
    event = json.loads(event_str)
    print(handler(event, None))
