import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import time
import logging
import os

TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']
dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)

log_level = logging.INFO
logging.basicConfig(format='%(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(log_level)


def handler(event, context):
    """AWS Lambda handler."""
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    # print('request: {}'.format(json.dumps(event)))
    api_path = event["resource"]
    # print(api_path)

    if api_path == "/matches/{proxy+}":
        logger.info("Processing " + api_path)
        if "proxy" in event["pathParameters"]:
            path = event["pathParameters"]["proxy"]
            logger.info("Proxy path " + path)
            path_tokens = path.split("/")
            if path_tokens[0].isnumeric():
                if int(path_tokens[0]) > 1006210516:  # rtcw release
                    pk = "match"
                    sk = path_tokens[0]
                    response = get_item(pk, sk, ddb_table, log_stream_name)

                    # logic specific to /matches/{match_id}
                    if "error" not in response:
                        data = json.loads(response["data"])
                        match_type_tokens = response['lsipk'].split("#")
                        data["type"] = "#".join(match_type_tokens[0:2])
                        data["match_round_id"] = response["sk"]
                    else:
                        data = response
            elif path_tokens[0] == "map":
                raise
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
                responses = get_range(pk, str(sklow), str(skhigh), ddb_table, log_stream_name)

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
            responses = get_range(pk, str(sklow), str(skhigh), ddb_table, log_stream_name)

            if "error" not in responses:
                # logic specific to /stats/player/{player_guid}
                data = []
                for line in responses:
                    data_line = json.loads(line["data"])
                    data_line["match_id"] = line["sk"]
                    data_line["type"] = line["gsi1pk"].replace("stats#", "")
                    data.append(data_line)
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
                data["type"] = response["gsi1pk"].replace("stats#", "")
            else:
                data = response

    if api_path == "/wstats/player/{player_guid}":
        logger.info("Processing " + api_path)
        if "player_guid" in event["pathParameters"]:
            guid = event["pathParameters"]["player_guid"]
            logger.info("Parameter: " + guid)
            pk = "wstats" + "#" + guid
            skhigh = int(time.time())
            sklow = skhigh - 60 * 60 * 24 * 30
            responses = get_range(pk, str(sklow), str(skhigh), ddb_table, log_stream_name)

            if "error" not in responses:
                # logic specific to /stats/player/{player_guid}
                data = []
                for line in responses:
                    data_line = json.loads(line["data"])
                    data_line["match_id"] = line["sk"]
                    # data_line["type"] = line["gsi1pk"].replace("wstats#","")
                    data.append(data_line)
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

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': json.dumps(data)
    }


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


def get_range(pk, sklow, skhigh, table, log_stream_name):
    """Get several items by pk and range of sk."""
    item_info = pk + ":" + sklow + " to " + skhigh + ". Logstream: " + log_stream_name
    try:
        response = table.query(KeyConditionExpression=Key('pk').eq(pk) & Key('sk').between(sklow, skhigh))
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if response['Count'] > 0:
            result = response['Items']
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
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
    event = json.loads(event_str)
    logger.info(handler(event, None))
