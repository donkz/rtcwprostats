import json
import boto3
from botocore.exceptions import ClientError
import logging
import os
import decimal
import datetime

if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
    MATCH_STATE_MACHINE = ""  # set this at debug time
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']
    MATCH_STATE_MACHINE = os.environ['RTCWPROSTATS_FUNNEL_STATE_MACHINE']
    
dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
sf_client = boto3.client('stepfunctions')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("retriever")
logger.setLevel(log_level)


def handler(event, context):
    """AWS Lambda handler."""
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    # print('request: {}'.format(json.dumps(event)))
    api_path = event["resource"]
    logger.info("incoming request " + api_path)
    
    if api_path == "/groups/add":
    
        data = {}
        submitter_ip = event.get("headers", {}).get("X-Forwarded-For","")
                   
        try:
            body = event["body"]
            body_json = json.loads(body)
            logger.info("submitted info: " + body)
        except:
            logger.error("Event was")
            logger.error(event)
            message = "Could not get body from the event"
            logger.error(message)
            data = make_error_dict(message,"")
        
        region = body_json.get("region",None)
        match_type = body_json.get("type",None)
        group_name = body_json.get("group_name",None)
        matches = body_json.get("matches",None)
        if not isinstance(matches, list):
            matches = None
        
        if not region or not match_type or not group_name or not matches:
            message = "One or more required parameters are missing or incorrect."
            logger.error(message)
            data = make_error_dict(message,"")
         
        if "error" not in data:
            Item = ddb_prepare_group_item(region, match_type, group_name.replace("#",""), matches, submitter_ip)
            ddb_put_item(Item, ddb_table)
            message = "Matches added to group " + str(group_name)
            data["response"] = message
            logger.info(message)
            execute_state_machine(group_name)
            
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
            },
        'body': json.dumps(data, default=default_type_error_handler)
        }

def execute_state_machine(group_name):
    try:
        response = sf_client.start_execution(
                stateMachineArn=MATCH_STATE_MACHINE,
                input='{"tasktype": "group_cacher","taskdetail": "' + group_name + '"}'
                )
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to start state machine for group_cacher group: " + group_name + "\n" + error_msg
        logger.error(message)
    else:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Started state machine " + response['executionArn'])
        else:
            logger.warning("Bad response from state machine " + str(response))
            message += "\nState machine failed."
            logger.error(message)
            

# https://stackoverflow.com/questions/63278737/object-of-type-decimal-is-not-json-serializable
def default_type_error_handler(obj):
    if isinstance(obj, decimal.Decimal):
        return int(obj)
    else:
        logger.error("Default type handler could not handle object" + str(obj))
    raise TypeError

def make_error_dict(message, item_info):
    """Make an error message for API gateway."""
    return {"error": message + " " + item_info}
    
def ddb_put_item(Item, table):
    try: 
        response = table.put_item(Item=Item)
    except ClientError as err:
        logger.error(err.response['Error']['Message'])
        logger.error("Item was: " + Item["pk"] + ":" + Item["sk"])
        raise
    else:
        pk = Item["pk"]
        sk = Item["sk"]
        http_code = "No http code"
        
        try: 
            http_code = response['ResponseMetadata']['HTTPStatusCode']
        except: 
            http_code = "Could not retrieve http code"
            logger.error("Could not retrieve http code from response\n" + str(response))
        
        if http_code != 200:
            logger.error("Unhandled HTTP Code " + str(http_code) + " while submitting item " + pk + ":" + sk + "\n" + str(response))
    return response

def ddb_prepare_group_item(region, match_type, group_name, matches, submitter_ip):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    group_item = {
        'pk'    : 'group',
        'sk'    : group_name + "#" + ts,
        'lsipk' : region + "#" + match_type + "#" + group_name + "#" + ts,
        'gsi1pk': "group",
        'gsi1sk': region + "#" + ts,
        'data'  : json.dumps(matches),
        "submitter_ip": submitter_ip
        }
    return group_item

# curl -X POST "https://rtcwproapi.donkanator.com/groups/add" -H "content-type: application/json" -H "pass: 123" -d "{ \"region\": \"eu\" , \"type\": \"6\", \"group_name\": \"gather16904\", \"matches\": [1634234950,1634236160,1634238542,1634238932] }"
if __name__ == "__main__":
    event = {
        "resource": "/groups/add",
        "headers": {
            "pass": "123",
            "X-Forwarded-For": "199.247.45.106",
        },
    "body": "{ \"region\": \"na\" , \"type\": \"6\", \"group_name\": \"gather16916\", \"matches\": [1634262652,1634264098,1634265028,1634265601]  }"
    }
    print(handler(event, None))
