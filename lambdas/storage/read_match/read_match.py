import logging
import boto3
import json
import time as _time
import os

from reader_writeddb import (
    # ddb_put_item,
    ddb_prepare_match_item,
    ddb_prepare_stats_items,
    ddb_prepare_statsall_item,
    ddb_prepare_gamelog_item,
    ddb_prepare_wstat_items,
    ddb_prepare_wstatsall_item,
    ddb_prepare_player_items,
    ddb_prepare_log_item,
    ddb_batch_write,
    ddb_update_server_record,
    ddb_prepare_server_item,
    ddb_get_server
)


# pip install --target ./ sqlalchemy
# import sqlalchemy

log_level = logging.INFO
logging.basicConfig(format='%(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(log_level)

dynamodb = boto3.resource('dynamodb')
client = boto3.client('dynamodb')

TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']
table = dynamodb.Table(TABLE_NAME)
s3 = boto3.client('s3')


def handler(event, context):
    """Read new incoming json and submit it to the DB."""
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

    integrity, message = integrity_checks(gamestats)
    if not integrity:
        return message

    server = ddb_get_server(gamestats['serverinfo']['serverName'], table)
    region = ""
    server_item = None
    if server:
        logger.info("Updating the server with +1 match")
        ddb_update_server_record(gamestats, table)
        region = server["region"]
    else:
        server_item = ddb_prepare_server_item(gamestats)

    if region == "":
        region = "na"  # TODO: insert region logic

    if len(gamestats.get("stats", 0)) == 2:
        team1_size = len(gamestats["stats"][0].keys())
        team2_size = len(gamestats["stats"][1].keys())
        total_size = team1_size + team2_size
        gametype = "notype"
        if 5 < total_size <= 7:  # normally 6, but shit happens 5-8
            gametype = "3"
        if 7 < total_size <= 14:  # normally 12, but shit happens 5-8
            gametype = "6"
        if 14 < total_size:
            gametype = "6plus"

    match_type = region + "#" + gametype
    logger.info("Setting the match_type to " + match_type)
    gamestats["match_type"] = match_type

    submitter_ip = gamestats.get("submitter_ip", "no.ip.in.file")

    items = []
    match_item = ddb_prepare_match_item(gamestats)
    match_id = str(match_item["sk"])
    stats_items = ddb_prepare_stats_items(gamestats)
    statsall_item = ddb_prepare_statsall_item(gamestats)
    gamelog_item = ddb_prepare_gamelog_item(gamestats)
    wstats_items = ddb_prepare_wstat_items(gamestats)
    wstatsall_item = ddb_prepare_wstatsall_item(gamestats)
    player_items = ddb_prepare_player_items(gamestats)
    log_item = ddb_prepare_log_item(match_id, file_key,
                                    len(match_item["data"]),
                                    len(stats_items),
                                    len(statsall_item["data"]),
                                    len(gamelog_item["data"]),
                                    len(wstats_items),
                                    len(wstatsall_item["data"]),
                                    len(player_items),
                                    # timestamp,
                                    submitter_ip)

    items.append(match_item)
    items.extend(stats_items)
    items.append(statsall_item)
    items.append(gamelog_item)
    items.extend(wstats_items)
    items.append(wstatsall_item)
    items.extend(player_items)
    items.append(log_item)
    if server_item:
        items.append(server_item)

    total_items = str(len(items))
    message = f"Sent {file_key} to database with {total_items} items. pk = match, sk = {match_id}"
    t1 = _time.time()
# =============================================================================
#     for Item in items:
#         response = None
#         try:
#             response = ddb_put_item(Item, table)
#         except Exception as ex:
#             template = "An exception of type {0} occurred. Arguments:\n{1!r}"
#             error_msg = template.format(type(ex).__name__, ex.args)
#             message = "Failed to load all records for a match " + file_key + "\n" + error_msg
#
#         if response["ResponseMetadata"]['HTTPStatusCode'] != 200:
#             message += "\nItem returned non 200 code " + Item["pk"] + ":" + Item["sk"]
# =============================================================================

    try:
        ddb_batch_write(client, table.name, items)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to load all records for a match " + file_key + "\n" + error_msg

    logger.info(message)
    time_to_write = str(round((_time.time() - t1), 3))
    logger.info(f"Time to write {total_items} items is {time_to_write} s")
    return message


def integrity_checks(gamestats):
    """Check if gamestats valid for any known things."""
    message = "Started integrity checks"
    integrity = True

    if "gameinfo" not in gamestats:
        integrity = False
        if "map_restart" in json.dumps(gamestats):
            message = "No gameinfo. Map was restarted. Aborting."
        else:
            message = "No gameinfo found."

    if "stats" not in gamestats:
        integrity = False
        message = "No stats in " + gamestats["gameinfo"].get('match_id', 'na')

    if isinstance(gamestats["stats"], dict):
        integrity = False
        message = gamestats["gameinfo"].get('match_id', 'na') + " stats is a dict. Expecting a list."

    return integrity, message


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
              "key": "test/gameStats_match_1613786926_round_2_mp_assault.json",
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
    print("Test result" + handler(event, None))
