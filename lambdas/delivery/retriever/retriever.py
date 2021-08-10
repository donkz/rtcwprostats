import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import time
import logging
import os
import decimal
import urllib.parse

if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']
    
dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)

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

    if api_path == "/matches/{proxy+}":
        if "proxy" in event["pathParameters"]:
            path = event["pathParameters"]["proxy"]
            logger.info("Proxy path " + path)
            path_tokens = path.split("/")
            if path_tokens[0].isnumeric() or len(path_tokens[0].split(','))>1:
                matches = path_tokens[0].split(",")
                item_list = []
                match_dups = []
                for match in matches:
                    if match.isnumeric() and int(match) > 1006210516:  # rtcw release
                        if match in match_dups:
                            logger.warning("Matches query string contains duplicate values. Dropping duplicates.")
                            continue
                        item_list.append({"pk": "match", "sk": match})
                        match_dups.append(match)
                        
                responses = get_batch_items(item_list, ddb_table, log_stream_name)
                # logic specific to /matches/{[match_id]}
                if "error" not in responses:
                    data = []
                    for response in responses:
                        match_data = json.loads(response["data"])
                        match_type_tokens = response['lsipk'].split("#")
                        match_data["type"] = "#".join(match_type_tokens[0:2])
                        match_data["match_round_id"] = response["sk"]
                        data.append(match_data)
                else:
                    data = response
            elif path_tokens[0] == "server" and len(path_tokens) > 1:
                pk_name = "gsi1pk"
                pk="match"
                begins_with = urllib.parse.unquote(path_tokens[1])
                logger.info("Searching for matches from server " + begins_with)
                index_name = "gsi1"
                skname = "gsi1sk"
                limit = 100
                acending = False
                responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, log_stream_name, limit, acending)

                # logic specific to /matches/recent/{days}
                if "error" not in responses:
                    data = []
                    for line in responses:
                        tmp_data = json.loads(line["data"])
                        match_type_tokens = line.get('lsipk',"##").split("#")
                        tmp_data["type"] = "#".join(match_type_tokens[0:2])
                        tmp_data["match_round_id"] = line["sk"]
                        data.append(tmp_data)
                else:
                    data = responses
            elif path_tokens[0] == "type":
                raise
            elif path_tokens[0] == "recent":
                if len(path_tokens) == 1:
                    days = 30
                elif path_tokens[1].isnumeric():
                    if int(path_tokens[1]) < 92:
                        days = path_tokens[1]
                    else:
                        days = 92  # 3 months or so
                logger.info("Number of days: " + str(days))
                pk = "match"
                skhigh = int(time.time())
                sklow = skhigh - 60 * 60 * 24 * int(days)
                responses = get_range(None, pk, str(sklow), str(skhigh), ddb_table, log_stream_name, 100)

                # logic specific to /matches/recent/{days}
                if "error" not in responses:
                    data = []
                    for line in responses:
                        tmp_data = json.loads(line["data"])
                        match_type_tokens = line['lsipk'].split("#")
                        tmp_data["type"] = "#".join(match_type_tokens[0:2])
                        tmp_data["match_round_id"] = line["sk"]
                        data.append(tmp_data)
                else:
                    data = responses

    if api_path == "/stats/player/{player_guid}":
        logger.info("Processing " + api_path)
        if "player_guid" in event["pathParameters"]:
            guid = event["pathParameters"]["player_guid"]
            logger.info("Parameter: " + guid)
            pk = "stats" + "#" + guid
            skhigh = int(time.time())
            sklow = skhigh - 60 * 60 * 24 * 30
            responses = get_range(None, pk, str(sklow), str(skhigh), ddb_table, log_stream_name, 40)

            if "error" not in responses:
                # logic specific to /stats/player/{player_guid}
                data = []
                for line in responses:
                    data_line = json.loads(line["data"])
                    data_line["match_id"] = line["sk"]
                    data_line["type"] = line["gsi1pk"].replace("stats#", "")
                    data.append(data_line)
            elif "Items do not exist" in responses["error"]:
                data = [] #taking a risk assuming the player simply did not play for 30 days
            else:
                data = responses

    if api_path == "/stats/{match_id}":
        logger.info("Processing " + api_path)
        if "match_id" in event["pathParameters"]:
            match_id = event["pathParameters"]["match_id"]
            logger.info("Parameter: " + match_id)
            pk = "statsall"
            sk = match_id
            response = get_item(pk, sk, ddb_table, log_stream_name)

            # logic specific to /stats/{match_id}
            if "error" not in response:
                data = {"statsall": json.loads(response["data"])}
                data["match_id"] = response["sk"]
                data["type"] = response["gsi1pk"].replace("statsall#", "")
            else:
                data = response

    if api_path == "/wstats/player/{player_guid}":
        logger.info("Processing " + api_path)
        if "player_guid" in event["pathParameters"]:
            guid = event["pathParameters"]["player_guid"]
            logger.info("Parameter: " + guid)
            pk = "wstats" + "#" + guid
            skhigh = int(time.time())
            sklow = skhigh - 60 * 60 * 24 * 30  # last 30 days only
            responses = get_range(None, pk, str(sklow), str(skhigh), ddb_table, log_stream_name, 40)

            if "error" not in responses:
                # logic specific to /stats/player/{player_guid}
                data = []
                for line in responses:
                    data_line = {}
                    data_line["wstats"] = json.loads(line["data"])
                    data_line["match_id"] = line["sk"]
                    # data_line["type"] = line["gsi1pk"].replace("wstats#","")
                    data.append(data_line)
            elif "Items do not exist" in responses["error"]:
                data = [] #taking a risk assuming the player simply did not play for 30 days
            else:
                data = responses

    if api_path == "/wstats/{match_id}":
        logger.info("Processing " + api_path)
        if "match_id" in event["pathParameters"]:
            match_id = event["pathParameters"]["match_id"]
            logger.info("Parameter: " + match_id)
            pk = "wstatsall"
            sk = match_id
            response = get_item(pk, sk, ddb_table, log_stream_name)

            # logic specific to /stats/{match_id}
            if "error" not in response:
                data = {"wstatsall": json.loads(response["data"])}
                data["match_id"] = response["sk"]
                # data["type"] = response["gsi1pk"].replace("wstats#","")
            else:
                data = response

    if api_path == "/gamelogs/{match_round_id}":
        logger.info("Processing " + api_path)
        if "match_round_id" in event["pathParameters"]:
            match_round_id = event["pathParameters"]["match_round_id"]
            logger.info("Parameter: " + match_round_id)
            pk = "gamelogs"
            sk = match_round_id
            response = get_item(pk, sk, ddb_table, log_stream_name)

            # logic specific to /gamelogs/{match_id}
            if "error" not in response:
                data = json.loads(response["data"])
            else:
                data = response

    if api_path == "/wstats/player/{player_guid}/match/{match_id}":
        logger.info("Processing " + api_path)
        if "match_id" in event["pathParameters"]:
            guid = event["pathParameters"]["player_guid"]
            match_id = event["pathParameters"]["match_id"]
            logger.info("Parameters: " + match_id + " " + guid)
            pk = "wstats" + "#" + guid
            sk = match_id
            response = get_item(pk, sk, ddb_table, log_stream_name)

            # logic specific to /stats/{match_id}
            if "error" not in response:
                data = {"wstats": json.loads(response["data"])}
                data["match_id"] = match_id
                data["player_guid"] = guid
            else:
                data = response

    if api_path == "/player/{player_guid}":
        logger.info("Processing " + api_path)
        if "player_guid" in event["pathParameters"]:
            player_guid = event["pathParameters"]["player_guid"]
            logger.info("Parameter: " + player_guid)
            pk = "player"
            sk = "playerinfo" + "#" + player_guid
            response = get_item(pk, sk, ddb_table, log_stream_name)

            # logic specific to /player/{player_guid}
            data = {}
            if "error" not in response:
                for key in response:
                    if key not in ["lsipk","pk","sk","gsi1pk","gsi1sk"]:
                        data[key]=response[key]
                data["player_guid"]=response["sk"].split("#")[1]
            else:
                data = response
                
    if api_path == "/player/search/{begins_with}":
        logger.info("Processing " + api_path)
        if "begins_with" in event["pathParameters"]:
            begins_with = "playerinfo#" + event["pathParameters"]["begins_with"]
            logger.info("Parameter: " + begins_with)
            pk_name = "pk"
            pk = "player"
            index_name = "lsi"
            skname="lsipk"
            responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname,  log_stream_name, 100, True)

            if "error" not in responses:
                # logic specific to /player/search/{begins_with}
                data = []
                for player in responses:
                    data_line = {}
                    
                    data_line["real_name"] = player.get("real_name","na")
                    data_line["guid"] = player["lsipk"].split("#")[2]
                    data_line["frequent_region"] = player.get("frequent_region","na")
                    data.append(data_line)
            else:
                data = responses
                
    if api_path == "/servers" or api_path == "/servers/detail":
        logger.info("Processing " + api_path)
        pk_name = "pk"
        pk = "server"
        limit =200
        responses = get_query_all(pk_name, pk, ddb_table, log_stream_name, limit)

        if "error" not in responses:
            # logic specific to /servers
            data = []
            for server in responses:
                data_line = {}
                
                data_line["server_name"] = server["sk"]
                data_line["region"] = server["region"]
                if server["lsipk"][0:2] ==  data_line["region"]:
                    data_line["last_submission"] = server["lsipk"][3:]
                else:
                    data_line["last_submission"] = '2021-07-31 23:59:59'
                data_line["submissions"]=int(server["submissions"])
                data_line["IP"]=server["data"]['serverIP']
                if api_path == "/servers/detail":
                    data_line["data"]=server["data"]
                data.append(data_line)
        else:
            data = responses

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
            },
        'body': json.dumps(data, default=default_type_error_handler)
        }

# https://stackoverflow.com/questions/63278737/object-of-type-decimal-is-not-json-serializable
def default_type_error_handler(obj):
    if isinstance(obj, decimal.Decimal):
        return int(obj)
    raise TypeError


def get_item(pk, sk, table, log_stream_name):
    """Get one dynamodb item."""
    item_info = pk + ":" + sk + ". Logstream: " + log_stream_name
    try:
        response = table.get_item(Key={'pk': pk, 'sk': sk})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if "Item" in response:
            result = response['Item']
        else:
            result = make_error_dict("[x] Item does not exist: ", item_info)
    return result


def get_range(index_name, pk, sklow, skhigh, table, log_stream_name, limit):
    """Get several items by pk and range of sk."""
    item_info = pk + ":" + sklow + " to " + skhigh + ". Logstream: " + log_stream_name
    try:
        if index_name:
            response = table.query(IndexName=index_name, KeyConditionExpression=Key('pk').eq(pk) & Key('sk').between(sklow, skhigh), Limit=limit,ReturnConsumedCapacity='NONE')
        else:
            response = table.query(KeyConditionExpression=Key('pk').eq(pk) & Key('sk').between(sklow, skhigh), Limit=limit,ReturnConsumedCapacity='NONE')
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if response['Count'] > 0:
            result = response['Items']
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result

def get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, log_stream_name, limit, ascending):
    """Get several items by pk and range of sk."""
    item_info = pk + ": begins with " + begins_with + ". Logstream: " + log_stream_name
    try:
        response = ddb_table.query(IndexName=index_name,KeyConditionExpression=Key(pk_name).eq(pk) & Key(skname).begins_with(begins_with), Limit=limit, ScanIndexForward=ascending)
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if response['Count'] > 0:
            result = response['Items']
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result

def get_query_all(pk_name, pk, ddb_table, log_stream_name, limit):
    """Get several items by pk."""
    item_info = pk + ". Logstream: " + log_stream_name
    try:
        response = ddb_table.query(KeyConditionExpression=Key(pk_name).eq(pk), Limit=limit)
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if response['Count'] > 0:
            result = response['Items']
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result

def get_batch_items(item_list, ddb_table, log_stream_name):
    """Get items in a batch."""
    dynamodb = boto3.resource('dynamodb')
    item_info = "get_batch_items. Logstream: " + log_stream_name
    try:
        response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list}})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if len(response["Responses"][ddb_table.name]) > 0:
            result = response["Responses"][ddb_table.name]
        else:
            result = make_error_dict("[x] Item does not exist: ", item_info)
    return result

def make_error_dict(message, item_info):
    """Make an error message for API gateway."""
    return {"error": message + " " + item_info}


if __name__ == "__main__":
    event_str = '''
    {
    "resource": "/stats/player/{player_guid}",
    "path": "/stats/player/1441314A80B76F",
    "httpMethod": "GET",
    "headers": null,
    "multiValueHeaders": null,
    "queryStringParameters": null,
    "multiValueQueryStringParameters": null,
    "pathParameters": {
        "player_guid": "1441314A80B76F"
    },
    "stageVariables": null
    }
    '''

    event_str = '''
    {
    "resource": "/matches/{proxy+}",
    "path": "/matches/wtf/kek/123",
    "httpMethod": "GET",
    "headers": null,
    "multiValueHeaders": null,
    "queryStringParameters": null,
    "multiValueQueryStringParameters": null,
    "pathParameters": {
        "proxy": "recent/100"
        }
    }
    '''
    
    event_str = '''
    {
    "resource": "/player/search/{begins_with}",
    "path": "/player/search/caka",
    "httpMethod": "GET",
    "headers": null,
    "multiValueHeaders": null,
    "queryStringParameters": null,
    "multiValueQueryStringParameters": null,
    "pathParameters": {
        "begins_with": "caka"
    },
    "stageVariables": null
    }
    '''
    
    event_str = '''
    {
    "resource": "/player/{player_guid}",
    "path": "/player/12345678",
    "httpMethod": "GET",
    "headers": null,
    "multiValueHeaders": null,
    "queryStringParameters": null,
    "multiValueQueryStringParameters": null,
    "pathParameters": {
        "player_guid": "123456787"
    },
    "stageVariables": null
    }
    '''
    
    event_str = '''
    {
     "resource":"/matches/{proxy+}",
     "pathParameters":{"proxy":"16098173561,16242774991,16103355022,16103355022x"}
    }
    '''
    event_str = '''
    {
     "resource":"/matches/{proxy+}",
     "pathParameters":{"proxy":"server/kekekke%20haha"}
    }
    '''
    
    event = json.loads(event_str)
    print(handler(event, None))
