import os
from datetime import datetime
import logging
from botocore.exceptions import ClientError
import json
import boto3
# import random
# import sys
# import pandas as pd
import time as _time


log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("summary_calc")
logger.setLevel(log_level)
debug = False  # very local debug
  
def process_rtcwpro_summary(ddb_table, ddb_client, match_id, log_stream_name):
    "RTCWPro pipeline specific logic."
    t1 = _time.time()
    error_encountered = False
    sk = match_id
    message = ""
    
    response = get_item("statsall", sk, ddb_table, log_stream_name)
    if "error" not in response:
        stats = json.loads(response["data"])
        logger.info("Retrieved statsall for " + str(len(stats)) + " players")
        if len(stats) == 2 and len(stats[0]) > 1: #stats grouped in teams in a list of 2 teams , each team over 1 player
            logger.info("Number of stats entries is erroneous, trying to merge teams")
            stats_tmp = stats[0].copy()
            stats_tmp.update(stats[1])
            stats = stats_tmp
            logger.info("New statsall has " + str(len(stats)) + " players")
            
    else:
        logger.error("Failed to retrieve statsall: " + sk)
        logger.error(json.dumps(response))
        error_encountered = True
        message += "Error in getting stats" + response["error"]
        return message
    
    response = get_item("wstatsall", sk, ddb_table, log_stream_name)
    if "error" not in response:
        wstats = json.loads(response["data"])
        logger.info("Retrieved wstatsall for " + str(len(wstats)) + " players")
    else:
        logger.error("Failed to retrieve wstatsall: " + sk)
        logger.error(json.dumps(response))
        error_encountered = True
        message += "Error in getting wstats" + response["error"]
        return message
    
    item_list = prepare_playerinfo_list(stats)
    response = get_batch_items(item_list, ddb_table, log_stream_name, "stats_sum, wstats_sum")

    stats_old = {}
    wstats_old= {}
    if "error" not in response:
        for result in response:
            guid = result["sk"].split("#")[1]
            if "stats_sum" in result:
                stats_old[guid] = result["stats_sum"]
            if "wstats_sum" in result:
                wstats_old[guid] = result["wstats_sum"]
    else:
        logger.error("Failed to retrieve any player summaries.")
        logger.error(json.dumps(response))
        error_ecnountered = True
        message += "Error in getting prepare_playerinfo_list" + response["error"]
        return message
   
    new_wstats = {}
    for wplayer_wrap in wstats:
        for wplayer_guid, wplayer in wplayer_wrap.items():
            new_wplayer = {}
            for weapon in wplayer:
                new_wplayer[weapon["weapon"]] = weapon
            new_wstats[wplayer_guid] = new_wplayer
    
    # build updated stats summaries
    stats_dict_updated = build_new_stats_summary(stats, stats_old)

    # submit updated stats summaries
    update_player_info_stats(ddb_table, stats_dict_updated, "stats_sum")
    
    # build updated wtats summaries
    wstats = new_wstats
    wstats_dict_updated = build_new_wstats_summary(wstats, wstats_old)

    # submit updated wstats summaries
    update_player_info_stats(ddb_table, wstats_dict_updated, "wstats_sum")
    
    time_to_write = str(round((_time.time() - t1), 3))
    logger.info(f"Time to process summaries is {time_to_write} s")
    message += "Records were summarized"
    return message

def build_new_stats_summary(stats, stats_old):
    """Add up new and old stats."""
    stats_dict_updated = {}
    for guid in stats:
        metrics = stats[guid]["categories"]
        stats_dict_updated[guid] = {}
        for metric in metrics:
            if guid not in stats_old:
                stats_dict_updated[guid][metric] = int(metrics[metric])
                continue
            if metric in stats_old[guid]:
                stats_dict_updated[guid][metric] = int(stats_old[guid][metric]) + int(metrics[metric])
            else:
                stats_dict_updated[guid][metric] = int(metrics[metric])
        stats_dict_updated[guid]["games"] = stats_old.get(guid,{}).get("games",0) + 1
    return stats_dict_updated

def build_new_wstats_summary(wstats, wstats_old):
    """Add up new and old stats."""
    wstats_dict_updated = {}
    for guid, wstat in wstats.items():
        wstats_dict_updated[guid] = {}
        for weapon, metrics in wstat.items():
            # print(weapon,weapon_info)
            wstats_dict_updated[guid][weapon] = {}

            for metric in metrics:
                if metric == "weapon":
                    continue
                #print(metric, metrics[metric])
                if guid not in wstats_old:
                    wstats_dict_updated[guid][weapon][metric] = int(metrics[metric])
                    continue
                if weapon not in wstats_old[guid]:
                    wstats_dict_updated[guid][weapon][metric] = int(metrics[metric])
                    continue
                if metric in wstats_old[guid][weapon]:
                    wstats_dict_updated[guid][weapon][metric] = int(wstats_old[guid][weapon][metric]) + int(metrics[metric])
                else:
                    wstats_dict_updated[guid][weapon][metric] = int(metrics[metric])
            wstats_dict_updated[guid][weapon]["games"] = wstats_old.get(guid,{}).get(weapon, {}).get("games",0) + 1
    return wstats_dict_updated
        
def wstat(new_wstats, guid, weapon, metric):
    """Safely get a number from a deeply nested dict."""
    if guid not in new_wstats:
        value = 0
    elif weapon not in new_wstats[guid]:
        value = 0
    elif metric not in new_wstats[guid][weapon]:
        value = 0
    else:
        value = new_wstats[guid][weapon][metric]
    return value


def make_error_dict(message, item_info):
    """Make an error message for API gateway."""
    return {"error": message + " " + item_info}


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


def prepare_playerinfo_list(stats):
    """Make a list of guids to retrieve from ddb."""
    item_list = []
    for guid, player_stats in stats.items():
        item_list.append({"pk": "player", "sk": "playerinfo#" + guid})
    return item_list


def get_batch_items(item_list, ddb_table, log_stream_name, extra_projections):
    """Get items in a batch."""
    dynamodb = boto3.resource('dynamodb')
    item_info = "get_batch_items. Logstream: " + log_stream_name
    try:
        response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list, 'ProjectionExpression': 'sk, real_name, ' + extra_projections}})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if len(response["Responses"][ddb_table.name]) > 0:
            result = response["Responses"][ddb_table.name]
        else:
            result = make_error_dict("[x] Item does not exist: ", item_info)
    return result


def update_player_info_stats(ddb_table, stats_dict_updated, stats_type):
    """Does not work because missing keys like "na#6#elo throw errors."""
    # Example: ddb_table.update_item(Key=Key, UpdateExpression="set elos.#eloname = :elo, elos.#gamesname = :games", ExpressionAttributeNames={"#eloname": "na#6#elo", "#gamesname": "na#6#games"}, ExpressionAttributeValues={':elo': 134, ':games' : 135})
    update_expression="set " + stats_type + " = :stat"
    for guid, new_stat in stats_dict_updated.items():
        logger.info("Updating player " + stats_type + ": " + guid)
        key = { "pk": "player" , "sk": "playerinfo#" + guid }
            
        expression_values = {':stat': new_stat}
        response = ddb_table.update_item(Key=key, 
                                         UpdateExpression=update_expression, 
                                         ExpressionAttributeValues=expression_values)