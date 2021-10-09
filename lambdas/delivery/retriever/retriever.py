import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import time
import logging
import os
import decimal
import urllib.parse
import datetime

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
    data = make_error_dict("Unhandled path: ", api_path)

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
                projections = "data, lsipk, sk"
                responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, limit, acending)

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
                error_msg = None
                logger.info("Processing " + api_path)
                if len(path_tokens) == 1:
                    error_msg = make_error_dict("Missing region and gametype", "")
                if len(path_tokens) >= 2:
                    if path_tokens[1].lower() in ['na','sa','eu','unk']:
                        region = path_tokens[1].lower()
                    else:
                        error_msg = make_error_dict("Invalid region", "")
                if len(path_tokens) == 3:
                    if path_tokens[2].lower() in ['3','6','6plus']:
                        teams = path_tokens[2].lower()
                    else:
                        error_msg = make_error_dict("Invalid match type", "")
                else:
                    teams = '6'
                    
                if error_msg:
                    data = error_msg
                else:
                    match_type = region + "#" + teams + "#"
                    logger.info("Processing match type " + match_type)
                    
                    pk = "match"
                    pk_name = "pk"
                    index_name = "lsi"
                    skname="lsipk"
                    begins_with = match_type
                    projections = "data, lsipk, sk"
                    responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, 100, False)
                    
                    # logic specific to /matches/type/...
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
                responses = get_range(None, pk, str(sklow), str(skhigh), ddb_table, log_stream_name, 100, False)

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
            responses = get_range(None, pk, str(sklow), str(skhigh), ddb_table, log_stream_name, 40, False)

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
            
            if match_id.isnumeric(): 
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
                    
            if len(match_id.split(','))>1:
                matches = match_id.split(",")
                item_list = []
                match_dups = []
                for match in matches:
                    if match.isnumeric() and int(match) > 1006210516:  # rtcw release
                        if match in match_dups:
                            logger.warning("Matches query string contains duplicate values. Dropping duplicates.")
                            continue
                        item_list.append({"pk": "statsall", "sk": match})
                        match_dups.append(match)
                        
                responses = get_batch_items(item_list, ddb_table, log_stream_name)
                
                # logic specific to /stats/{match_id} with match array
                if "error" not in responses:
                    data = []
                    for match_stat in responses: 
                        data_line = {match_stat["sk"]: json.loads(match_stat["data"])}
                        data_line["match_id"] = match_stat["sk"]
                        data_line["type"] = match_stat["gsi1pk"].replace("statsall#", "")
                        data.append(data_line)
                else:
                    data = responses

    if api_path == "/wstats/player/{player_guid}":
        logger.info("Processing " + api_path)
        if "player_guid" in event["pathParameters"]:
            guid = event["pathParameters"]["player_guid"]
            logger.info("Parameter: " + guid)
            pk = "wstats" + "#" + guid
            skhigh = int(time.time())
            sklow = skhigh - 60 * 60 * 24 * 30  # last 30 days only
            responses = get_range(None, pk, str(sklow), str(skhigh), ddb_table, log_stream_name, 40, False)

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

            pk = "player" + "#" + player_guid
            response = get_items_pk(pk, ddb_table, log_stream_name)
            data = process_player_response(response)
            
    if api_path == "/leaders/{category}/region/{region}/type/{type}" or api_path == "/leaders/{category}/region/{region}/type/{type}/limit/{limit}":
        logger.info("Processing " + api_path)

        category = event["pathParameters"]["category"]
        region = event["pathParameters"]["region"]
        type_ = event["pathParameters"]["type"]
        
        if api_path == "/leaders/{category}/region/{region}/type/{type}/limit/{limit}":
            limit = int(event["pathParameters"]["limit"])
        else:
            limit = 20
            
        logger.info("Parameters: " + category + " " + region + " " + type_)

        pk = "leader" + category + "#" + region + "#" + type_
        projection = "pk, gsi1sk, real_name, games"
        
        response = get_leaders(pk, ddb_table, projection, limit, log_stream_name)
        data = process_leader_response(response)
    
    if api_path == "/eloprogress/player/{player_guid}/region/{region}/type/{type}":
        logger.info("Processing " + api_path)

        player_guid = event["pathParameters"]["player_guid"]
        region = event["pathParameters"]["region"]
        type_ = event["pathParameters"]["type"]
        limit = 40
        
        logger.info("Parameters: " + player_guid + " " + region + " " + type_)
        
        pk_name = "pk"
        pk = "eloprogress#" + player_guid
        index_name = None
        skname="sk"
        begins_with = region + "#" + type_ + "#"
        ascending = False
        projections = "data, gsi1sk, elo, performance_score, real_name"
        response = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, limit, ascending)  
        data = process_eloprogress_response(response)
    
    if api_path == "/eloprogress/match/{match_id}":
        logger.info("Processing " + api_path)

        match_id = event["pathParameters"]["match_id"]
        limit = 20  # how many people can there be?
        
        logger.info("Parameters: " + match_id)
        
        pk_name = "gsi1pk"
        pk = "eloprogressmatch"
        index_name = "gsi1"
        skname="gsi1sk"
        begins_with = match_id
        ascending = False
        projections = "data, gsi1sk, elo, performance_score, real_name"
        response = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, limit, ascending)  
        data = process_eloprogress_response(response)
                
    if api_path == "/player/search/{begins_with}":
        logger.info("Processing " + api_path)
        if "begins_with" in event["pathParameters"]:
            begins_with = "realname#" + event["pathParameters"]["begins_with"]
            logger.info("Parameter: " + begins_with)
            pk_name = "gsi1pk"
            pk = "realname"
            index_name = "gsi1"
            skname="gsi1sk"
            projections = "data, pk, updated"
            responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, 100, True)

            if "error" not in responses:
                # logic specific to /player/search/{begins_with}
                data = []
                for player in responses:
                    data_line = {}
                    data_line["real_name"] = player.get("data","na")
                    data_line["guid"] = player["pk"].split("#")[1]
                    data_line["last_seen"] = player.get("updated","2021-07-31T22:21:34.211247")
                    data.append(data_line)
            else:
                data = responses
                
                
    if api_path == "/aliases/search/{begins_with}":
        logger.info("Processing " + api_path)

        begins_with = event["pathParameters"]["begins_with"]
        logger.info("Parameter: " + begins_with)
        pk_name = "gsi1pk"
        pk = "aliassearch2"
        index_name = "gsi1"
        skname="gsi1sk"
        projections = "sk, gsi1sk, last_seen, lsipk, real_name"
        responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, 40, True)
        data = process_alias_responses(api_path, responses)
        
            
    if api_path == "/aliases/player/{player_guid}":
        logger.info("Processing " + api_path)

        player_guid = event["pathParameters"]["player_guid"]
        logger.info("Parameter: " + player_guid)
        pk_name = "pk"
        pk = "aliases"
        index_name = None
        skname="sk"
        begins_with = player_guid + "#"
        limit = 40
        ascending = False
        projections = "sk, gsi1sk, last_seen, lsipk, real_name"
        responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, limit, ascending)  
        data = process_alias_responses(api_path, responses)
    
    if api_path == "/aliases/recent/limit/{limit}":
        logger.info("Processing " + api_path)

        limit_str = event["pathParameters"]["limit"]
        logger.info("Parameter: " + limit_str)
        pk_name = "pk"
        pk = "aliases"
        index_name = "lsi"
        skname="lsipk"
        begins_with = "1"  # TODO!!!!
        
        limit = 30
        if limit_str.isdigit():
            limit = min(int(limit_str),101)
            
        ascending = False
        projections = "sk, gsi1sk, last_seen, lsipk, real_name"
        responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, limit, ascending)  
        data = process_alias_responses(api_path, responses)
                
    if api_path == "/servers" or api_path == "/servers/detail":
        logger.info("Processing " + api_path)
        pk_name = "pk"
        pk = "server"
        limit =200
        responses = get_query_all(pk_name, pk, ddb_table, log_stream_name, limit)
        data = process_server_responses(api_path, responses)
    
    if api_path == "/servers/region/{region}" or api_path == "/servers/region/{region}/active":
        logger.info("Processing " + api_path)
        region = event["pathParameters"]["region"]
        logger.info("Parameter: " + region)
        pk_name = "pk"
        pk = "server"
        index_name = "lsi"
        skname="lsipk"
        limit = 200
        ascending = False
             
        if api_path == "/servers/region/{region}":
            begins_with = region
            projections = "sk, region, lsipk, submissions, data"
            responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, limit, ascending)
        
        if api_path == "/servers/region/{region}/active":
            dt = datetime.datetime.now() - datetime.timedelta(days=30)
            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            sklow = region + "#" + dt_str
            skhigh = region + "#" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            responses = get_range(index_name, pk, str(sklow), str(skhigh), ddb_table, log_stream_name, limit, ascending)

        data = process_server_responses(api_path, responses)
    
    # if api_path == "/groups/add":
        # this functionality is in delivery_writer.py

    if api_path == "/groups/{proxy+}":
        # data = event
        path = event.get("pathParameters",{}).get("proxy","")
        logger.info("Proxy path " + path)
        path_tokens = path.split("/")
        data = {}
        projections = "sk, data"
        
        if len(path_tokens) == 2 and path_tokens[0] == "group_name" and len(path_tokens[0].strip())>0:
            logger.info("Parameter: /groups/group_name/{group_name}")
            pk_name = "pk"
            pk = "group"
            index_name = None
            skname="sk"
            begins_with = path_tokens[1]
            ascending = False
            responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, 100, ascending)  
            
            if "error" not in responses:
                data = {}
                for group in responses:
                    data[group["sk"].split("#")[0]] = json.loads(group["data"])
            else:
                data = responses    
        
        if len(path_tokens) == 2 and path_tokens[0] == "region" and path_tokens[1] in ["sa","na","eu","unk"]:
            logger.info("Parameter: /groups/region/{region_name}")
            pk_name = "pk"
            pk = "group"
            index_name = "lsi"
            skname="lsipk"
            begins_with = path_tokens[1]
            ascending = False
            responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, 100, ascending)  
            
            if "error" not in responses:
                data = {}
                for group in responses:
                    data[group["sk"].split("#")[0]] = json.loads(group["data"])
            else:
                data = responses    
        
        if (len(path_tokens) == 4 and path_tokens[0] == "region" and path_tokens[1] in ["sa","na","eu","unk"] and 
           path_tokens[2] == "type" and path_tokens[3] in ["3","6","6plus"]) :
            logger.info("Parameter: /groups/region/{region_name}/type/{match_type}")
            pk_name = "pk"
            pk = "group"
            index_name = "lsi"
            skname="lsipk"
            begins_with = path_tokens[1] + "#" + path_tokens[3]
            ascending = False
            responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, 100, ascending)  
            
            if "error" not in responses:
                data = {}
                for group in responses:
                    data[group["sk"].split("#")[0]] = json.loads(group["data"])
            else:
                data = responses
                
        if (len(path_tokens) == 6 and path_tokens[0] == "region" and path_tokens[1] in ["sa","na","eu","unk"] and 
           path_tokens[2] == "type" and path_tokens[3] in ["3","6","6plus"] and
           path_tokens[4] == "group_name" and len(path_tokens[5].strip()) > 0 ):
            logger.info("Parameter: /groups/region/{region_name}/type/{match_type}/group_name/{group_name}")
            pk_name = "pk"
            pk = "group"
            index_name = "lsi"
            skname="lsipk"
            begins_with = path_tokens[1] + "#" + path_tokens[3] + "#" + path_tokens[5]
            ascending = False
            responses = get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, 100, ascending)  
            
            if "error" not in responses:
                data = {}
                for group in responses:
                    data[group["sk"].split("#")[0]] = json.loads(group["data"])
            else:
                data = responses
        
        if len(data) == 0:
            data = make_error_dict("Could not match path:", path)
        

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
    else:
        logger.error("Default type handler could not handle object" + str(obj))
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

def get_items_pk(pk, table, log_stream_name):
    """Get several items by pk."""
    item_info = pk + " Logstream: " + log_stream_name
    try:
        response = table.query(KeyConditionExpression=Key('pk').eq(pk), Limit=100,ReturnConsumedCapacity='NONE')
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if response['Count'] > 0:
            result = response['Items']
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result

def get_leaders(pk, table, projection, limit, log_stream_name):
    """Get several items by pk."""
    item_info = pk + " Logstream: " + log_stream_name
    try:
        response = table.query(IndexName='gsi1', KeyConditionExpression=Key('gsi1pk').eq(pk), ProjectionExpression=projection, ScanIndexForward=False, Limit=limit,ReturnConsumedCapacity='NONE')
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if response['Count'] > 0:
            result = response['Items']
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result


def get_range(index_name, pk, sklow, skhigh, table, log_stream_name, limit, ascending):
    """Get several items by pk and range of sk."""
    item_info = pk + ":" + sklow + " to " + skhigh + ". Logstream: " + log_stream_name
    try:
        if index_name:
            response = table.query(IndexName=index_name, KeyConditionExpression=Key('pk').eq(pk) & Key("lsipk").between(sklow, skhigh), Limit=limit,ReturnConsumedCapacity='NONE', ScanIndexForward=ascending)
        else:
            response = table.query(KeyConditionExpression=Key('pk').eq(pk) & Key('sk').between(sklow, skhigh), Limit=limit,ReturnConsumedCapacity='NONE', ScanIndexForward=ascending)
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if response['Count'] > 0:
            result = response['Items']
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result

def get_begins(pk_name, pk, begins_with, ddb_table, index_name, skname, projections, log_stream_name, limit, ascending):
    """Get several items by pk and range of sk."""
    item_info = pk + ": begins with " + begins_with + ". Logstream: " + log_stream_name
    projections = projections.replace("data", "#data_value").replace("region", "#region_value")
    
    expressionAttributeNames = {} # knee deep
    if '#data_value' in projections:
        expressionAttributeNames['#data_value'] = 'data'
    if '#region_value' in projections:
        expressionAttributeNames['#region_value'] = 'region'
        
    try:
        if index_name:
            if "_value" in projections:  # wish there was a way to not error over unuzed projection names
                response = ddb_table.query(IndexName=index_name,KeyConditionExpression=Key(pk_name).eq(pk) & Key(skname).begins_with(begins_with), ProjectionExpression=projections, ExpressionAttributeNames=expressionAttributeNames, Limit=limit, ScanIndexForward=ascending)
            else:
                response = ddb_table.query(IndexName=index_name,KeyConditionExpression=Key(pk_name).eq(pk) & Key(skname).begins_with(begins_with), ProjectionExpression=projections, Limit=limit, ScanIndexForward=ascending)
        else:
            if "_value" in projections:
                response = ddb_table.query(KeyConditionExpression=Key(pk_name).eq(pk) & Key(skname).begins_with(begins_with),ProjectionExpression=projections, ExpressionAttributeNames=expressionAttributeNames, Limit=limit, ScanIndexForward=ascending)
            else:
                response = ddb_table.query(KeyConditionExpression=Key(pk_name).eq(pk) & Key(skname).begins_with(begins_with),ProjectionExpression=projections, Limit=limit, ScanIndexForward=ascending)
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

def process_server_responses(api_path, responses):
    if "error" not in responses:
        # logic specific to /servers
        data = []
        for server in responses:
            data_line = {}
            
            data_line["server_name"] = server["sk"]
            data_line["region"] = server["region"]
            if server["lsipk"].split("#")[0] ==  data_line["region"]:
                data_line["last_submission"] = server["lsipk"].split("#")[1]
            else:
                data_line["last_submission"] = '2021-07-31 23:59:59'
            data_line["submissions"]=int(server["submissions"])
            data_line["IP"]=server["data"]['serverIP']
            if api_path == "/servers/detail":
                data_line["data"]=server["data"]
            data.append(data_line)
    else:
        data = responses
    return data

def process_player_response(response):
    data = {}
    data["elos"] = {}
    data["aggstats"] = {}
    data["aggwstats"] = {}
    data["kdr"] = {}
    data["acc"] = {}
    data["real_name"] = ""
    data["last_seen"] = ""

    if "error" in response:
        data = response
    else:
        try:
            for item in response:
                if "elo#" in  item["sk"]:
                    data["elos"][item["sk"].replace("elo#","")] = {}
                    data["elos"][item["sk"].replace("elo#","")]["elo"] = int(item["data"])
                    data["elos"][item["sk"].replace("elo#","")]["games"] = int(item["games"])
                if "realname" in  item["sk"]:
                    data["real_name"] = item["data"]
                    data["last_seen"] = item.get("updated","2021-07-31T22:21:34.211247")
                if "aggstats#" in  item["sk"]:
                    data["aggstats"][item["sk"].replace("aggstats#","")] = item["data"]
                    data["kdr"][item["gsi1pk"].replace("leaderkdr#","")] = float(item["gsi1sk"])
                if "aggstats#" in  item["sk"]:
                    data["aggwstats"][item["sk"].replace("aggwstats#","")] = item["data"]
                    data["acc"][item["gsi1pk"].replace("leaderacc#","")] = float(item["gsi1sk"])
        except:
            item_info = "unkown"
            if len(response) > 0:
                if "pk" in response[0]:
                   item_info = item["pk"]
            data["error"] = "Could not process player response for " + item_info
            logger.error(data["error"])
    return data

def process_leader_response(response):
    data = []
    if "error" in response:
        data = response
    else:
        try:
            for item in response:
               leader_line = {}
               # leader_line['updated'] = item["updated"]
               leader_line['real_name'] = item.get("real_name","no_name#")
               leader_line['value'] = float(item["gsi1sk"])
               leader_line['guid'] = item["pk"].split("#")[1]
               leader_line['games'] = int(item.get("games",0)) 
               data.append(leader_line)
        except:
            item_info = "unkown"
            if len(response) > 0:
                if "pk" in response[0]:
                   item_info = item["pk"]
            data = make_error_dict("Could not process leader response.", item_info)
            logger.error(data["error"])
    return data

def process_eloprogress_response(response):
    data = []
    if "error" in response:
        data = response
    else:
        try:
            for item in response:
               elo_delta = {}
               # leader_line['updated'] = item["updated"]
               elo_delta['value'] = int(item["data"])
               elo_delta['elo'] = int(item["elo"])
               elo_delta['match_id'] = int(item.get("gsi1sk",0)) 
               elo_delta['real_name'] = item.get("real_name","no_name#")
               data.append(elo_delta)
        except:
            item_info = "unkown"
            if len(response) > 0:
                if "pk" in response[0]:
                   item_info = item["pk"]
            data = make_error_dict("Could not process leader response.", item_info)
            logger.error(data["error"])
    return data
    
def process_alias_responses(api_path, responses):
    data = []
    if "error" in responses:
        data = responses
    else:
        # logic specific to /aliases/*
        for player in responses:
            data_line = {}
            
            data_line["last_seen"] = player.get("last_seen","na")
            data_line["real_name"] = player.get("real_name","na")
            data_line["alias"] = player.get("gsi1sk","na")
            data_line["last_match"] = player["lsipk"].split("#")[0]
            data_line["guid"] = player["sk"].split("#")[0]
            data.append(data_line)
    return data
                       
        

if __name__ == "__main__":
    event_str = '''
    {
    "resource": "/stats/player/{player_guid}",
    "pathParameters": {
        "player_guid": "08ce652ba1a7c8c6c3ff101e7c390d20"
    },
    }
    '''
    
    event_stats_csv = '''
    {
    "resource": "/stats/{match_id}",
    "pathParameters": {
        "match_id": "1630476331,1630475541,1630474233,1630472750"
    },
    }
    '''
    
    event_str = '''
    {
    "resource": "/matches/{proxy+}",
    "pathParameters": {"proxy": "recent/100"}
    }
    '''
    
    event_str_player_search = '''
    {
    "resource": "/player/search/{begins_with}",
    "pathParameters": {"begins_with": "jam"}
    }
    '''
    
    event_str_player_guid = '''
    {
    "resource": "/player/{player_guid}",
    "pathParameters": {"player_guid": "fbe2ed832f8415efbaaa5df10074484a" }
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
    
    event_str_match_type = '''
    {
     "resource":"/matches/{proxy+}",
     "pathParameters":{"proxy":"type/na"}
    }
    '''
    
    event_str_group_name = '''
    {
     "resource": "/groups/{proxy+}",
     "pathParameters": {
         "proxy": "group_name/bather"
         }
     }
    '''
    
    event_str_group_region = '''
    {
     "resource": "/groups/{proxy+}",
     "pathParameters": {
         "proxy": "region/na"
         }
     }
    '''
    
    event_str_group_region_type = '''
    {
     "resource": "/groups/{proxy+}",
     "pathParameters": {
         "proxy": "region/na/type/6"
         }
     }
    '''
    
    event_str_group_region_type_name = '''
    {
     "resource": "/groups/{proxy+}",
     "pathParameters": {
         "proxy": "region/eu/type/6/group_name/bath"
         }
     }
    '''
    event_str_leader = '''
    {
      "resource": "/leaders/{category}/region/{region}/type/{type}",
      "pathParameters": {
        "category": "elo",
        "region": "sa",
        "type": "6"
      }
    }
    '''
    
    event_str_leader_limit = '''
    {
      "resource": "/leaders/{category}/region/{region}/type/{type}/limit/{limit}",
      "pathParameters": {
        "category": "elo",
        "region": "na",
        "type": "6",
        "limit": "5"
      }
    }
    '''
    
    event_str_eloprogress_guid = '''
    {
      "resource": "/eloprogress/player/{player_guid}/region/{region}/type/{type}",
      "pathParameters":{"player_guid":"8e6a51baf1c7e338a118d9e32472954e","region":"na","type":"6"}
    }
    '''
    event_str_eloprogress_match = '''
    {
      "resource": "/eloprogress/match/{match_id}",
      "pathParameters":{"match_id":"1629382279"}
    }
    '''
    
    event_str_aliases_guid = '''
    {
      "resource": "/aliases/player/{player_guid}",
      "pathParameters":{"player_guid":"8e6a51baf1c7e338a118d9e32472954e"}
    }
    '''
    # 8e6a51baf1c7e338a118d9e32472954e
    
    event_str_aliases_search = '''
    {
      "resource": "/aliases/search/{begins_with}",
      "pathParameters":{"begins_with":"fatal"}
    }
    '''
 
    event = json.loads(event_str_player_search)
    print(handler(event, None))
