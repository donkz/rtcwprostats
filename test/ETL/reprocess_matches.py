import boto3
import logging
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from botocore.exceptions import ClientError
import time
import json

# for local testing use actual table
# for lambda execution runtime use dynamic reference
TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
log_stream_name = "local"

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('reprocess_matches')
logger.setLevel(log_level)

sf_client = boto3.client('stepfunctions')

def get_matches_for_prefix(ddb_table, skip_servers, begins_with="na#6#", limit=2):
    index_name = "lsi"
    pk = "match"
    skname = "lsipk"
    begins_with = begins_with
    limit = limit
    response = ddb_table.query(IndexName=index_name, KeyConditionExpression=Key('pk').eq(pk) & Key(skname).begins_with(begins_with),
                               ExpressionAttributeNames={"#data_value": "data"},
                               ProjectionExpression="#data_value",
                               Limit=limit, 
                               ScanIndexForward=False,
                               ReturnConsumedCapacity='TOTAL')
    logger.info("Query ConsumedCapacity:" + str(response['ConsumedCapacity']['CapacityUnits']))
    
    matches = []
    if response['Count'] > 0:
       for record in response["Items"]:
           data_json = json.loads(record['data'])
           if data_json.get("round","") != "2":
               continue
           match_id = data_json['match_id']
           server_name = data_json.get('server_name',"no_server_name")
           if server_name in skip_servers:
               continue
           date_time_human = data_json.get('date_time_human',"no_date_time")
           print(match_id, server_name.ljust(30), date_time_human)
           matches.append(match_id)
    return matches

def execute_sf(match_id):
    request = None
    MATCH_STATE_MACHINE= "arn:aws:states:us-east-1:793070529856:stateMachine:ProcessMatchDataA1C168FE-zrtv8AF2Cd8C"
    try:
        response = sf_client.start_execution(
                stateMachineArn=MATCH_STATE_MACHINE,
                input='{"matchid": "' + match_id + '","roundid": ' + '2' + '}'
                )
        request = response['ResponseMetadata']['RequestId']
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to start state machine for " + match_id + "\n" + error_msg
        logger.error(message)
    else:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Started state machine " + response['executionArn'])
        else:
            logger.warning("Bad response from state machine " + str(response))
            message += "\nState machine failed."
    return request

def do():
    #skip_servers = ["RTCWPro-Host","WolfHost","^7Quakecon 2021 - London #1"]
    skip_servers = ["RTCWPro-Host","WolfHost"]
    matches = get_matches_for_prefix(ddb_table, skip_servers, "eu#3#", 600)
    matches.sort()
    requests = []
    for match_id in matches:
        request = execute_sf(match_id)
        if request:
            requests.append(request)
        time.sleep(6)  # give just a chance previous SF time to finish 
    return requests