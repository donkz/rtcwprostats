import logging
from botocore.exceptions import ClientError
import json
import boto3
import time as _time
from boto3.dynamodb.conditions import Key

from group_cache_matchinfo import build_teams, build_new_match_summary, convert_stats_to_dict

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("group_calc")
logger.setLevel(log_level)
  
def process_rtcwpro_summary(ddb_table, ddb_client, group_name, log_stream_name):
    "RTCWPro pipeline specific logic."
    t1 = _time.time()
    message = ""
    
    group_response = ddb_table.query(KeyConditionExpression=Key("pk").eq("group") & Key("sk").begins_with(group_name), Limit=1, ScanIndexForward=False)
    if len(group_response.get("Items",[])) > 0:
        matches = json.loads(group_response["Items"][0]["data"])
    
    item_list = []
    item_list.extend(prepare_stats_item_list(matches,"statsall"))
    item_list.extend(prepare_stats_item_list(matches,"wstatsall"))
    item_list.extend(prepare_matches_item_list(matches))
    responses = get_batch_items(item_list, ddb_table, log_stream_name)

    match_dict= {}
    stats_dict = {}
    wstats_dict = {}
    match_region_type = ""
    if "error" not in responses and len(responses) > 0:
        for response in responses:
            if response["pk"] == "wstatsall":
                wstats_dict[response["sk"]] = json.loads(response["data"])
            if response["pk"] == "statsall":
                match_region_type = response["gsi1pk"].replace("statsall#", "")
                stats_dict[response["sk"]] = json.loads(response["data"])
            if response["pk"] == "match":
                match_dict[response["sk"]] = json.loads(response["data"])
    else:
        logger.error("Failed to retrieve any group data:" + group_name)
        logger.error(json.dumps(response))
        message += "Error in getting stats" + response["error"]
        return message
    
    new_total_stats = {}
    for match_id, stats in stats_dict.items():
        new_total_stats[match_id] = convert_stats_to_dict(stats)
    
    new_total_wstats = {}
    for match_id, wstats in wstats_dict.items():
        new_wstats = {}
        for wplayer_wrap in wstats:
            for wplayer_guid, wplayer in wplayer_wrap.items():
                new_wplayer = {}
                for weapon in wplayer:
                    weapon_code = weapon["weapon"]
                    new_wplayer[weapon_code] = weapon
                new_wstats[wplayer_guid] = new_wplayer
        new_total_wstats[match_id] = new_wstats
    
    
    # build updated stats summaries
    stats_old = {}
    for match_id, stats in new_total_stats.items():
        stats_dict_updated = build_new_stats_summary(stats, stats_old)
        # print(stats_dict_updated["8ff4ecf7bd1b87edad5383efcfdb3c8d"]["kills"])
        stats_old = stats_dict_updated.copy()
    
    teamA, teamB, aliases, team_mapping, alias_team_str = build_teams(new_total_stats)
    
    # build updated wtats summaries
    wstats_old = {} #here we always start fresh
    for match_id, wstats in new_total_wstats.items():
        wstats_dict_updated = build_new_wstats_summary(wstats, wstats_old)
        # print(wstats_dict_updated["8ff4ecf7bd1b87edad5383efcfdb3c8d"]["MP-40"])
        wstats_old = wstats_dict_updated.copy()
    wstats_standard_response = emulate_wstats_api(wstats_dict_updated, group_name)
    
    match_summary = build_new_match_summary(match_dict, team_mapping)
    stats_standard_response = emulate_stats_api(stats_dict_updated, teamA, teamB, aliases, match_region_type, group_name, match_summary)
    
    group_item = ddb_prepare_group_item(group_response, alias_team_str, match_summary)
    stats_item = ddb_prepare_stat_item("stats", stats_standard_response, match_region_type, group_name)
    wstats_item = ddb_prepare_stat_item("wstats", wstats_standard_response, match_region_type, group_name)
    
    # submit updated summaries
    items = []
    items.append(stats_item)
    items.append(wstats_item)
    items.append(group_item)

    try:
        ddb_batch_write(ddb_client, ddb_table.name, items)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to submit aggregate stats for a group to database " + group_name + "\n" + error_msg
        logger.info(message)
    else:
        message = "Elo progress records inserted.\n"
    
    time_to_write = str(round((_time.time() - t1), 3))
    logger.info(f"Time to process summaries is {time_to_write} s")
    message += "Group was cached"
    return message


def emulate_stats_api(stats_dict_updated, teamA, teamB, aliases, match_region_type, group_name, match_summary):
    """ Convert current wstats summary to json format consistent with raw match data."""
    response = {}
    response["statsall"] = []
    response["match_id"] = "group " + group_name
    response["type"] = match_region_type
    response["match_summary"] = match_summary
    
    for guid, player_stat in stats_dict_updated.items():
        
        team = "Allied"
        if guid in teamA:
            team = "Axis"
            
        player_wrapper = {}
        player_wrapper[guid] = {}
        player_wrapper[guid]["alias"] = aliases.get(guid,"name_error#")
        player_wrapper[guid]["team"] = team
        player_wrapper[guid]["start_time"] = 0
        player_wrapper[guid]["num_rounds"] = 1
        player_wrapper[guid]["categories"] = player_stat
        player_wrapper[guid]["jsonGameStatVersion"] = "0.1.3"  # That's what emulated anyway
        response["statsall"].append(player_wrapper)
    return response


def emulate_wstats_api(wstats_dict_updated, group_name):
    """ Convert current wstats summary to json format consistent with raw match data."""
    response = {}
    response["wstatsall"] = wstats_dict_updated
    response["match_id"] = "group " + group_name
    return response
                         

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
                if metric not in ["accuracy","efficiency", "killpeak"]:
                    stats_dict_updated[guid][metric] = int(stats_old[guid][metric]) + int(metrics[metric])
            else:
                stats_dict_updated[guid][metric] = int(metrics[metric])
        
        new_acc = metrics["hits"]/metrics["shots"]
        stats_dict_updated[guid]["accuracy"] = int(new_acc)
        
        efficiency = 100*stats_dict_updated[guid]["kills"]/(stats_dict_updated[guid]["kills"] + stats_dict_updated[guid]["deaths"])                
        stats_dict_updated[guid]["efficiency"] = int(efficiency)
        stats_dict_updated[guid]["killpeak"] = max(stats_dict_updated[guid].get("killpeak",0),metrics.get("killpeak",0))
    
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
        

def make_error_dict(message, item_info):
    """Make an error message for API gateway."""
    return {"error": message + " " + item_info}

def prepare_playerinfo_list(stats, sk):
    """Make a list of guids to retrieve from ddb."""
    item_list = []
    for guid, player_stats in stats.items():
        item_list.append({"pk": "player#" + guid, "sk": sk})
    return item_list


def get_batch_items(item_list, ddb_table, log_stream_name):
    """Get items in a batch."""
    dynamodb = boto3.resource('dynamodb')
    item_info = "get_batch_items. Logstream: " + log_stream_name
    try:
        response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list, 'ProjectionExpression': 'pk, sk, #data_value, gsi1pk', 'ExpressionAttributeNames': {'#data_value': 'data'}}}, ReturnConsumedCapacity='NONE') #10 RCU 
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if len(response["Responses"][ddb_table.name]) > 0:
            result = response["Responses"][ddb_table.name]
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result        
        
def ddb_prepare_stat_item(stat_type, stats, match_region_type, group_name):
    """ Prepare an item in dynamodb format. """
    item = {
            'pk'            : "groupcache#" + stat_type,
            'sk'            : group_name,
            'data'          : json.dumps(stats)
        }
    return item

def ddb_prepare_group_item(group_response, alias_team_str, match_summary):
    """Enhance group item with more info and completion stamp."""
    item = group_response['Items'][0]
    item["duration_nice"] = match_summary["duration_nice"]
    item["finish_human"] = match_summary["finish_human"]
    item["games"] = int(match_summary["games"])
    item["teams"] = alias_team_str
    item["cached"] = "Yes"
    return item
    
def create_batch_write_structure(table_name, items, start_num, batch_size):
    """
    Create item structure for passing to batch_write_item
    :param table_name: DynamoDB table name
    :param items: large collection of items
    :param start_num: Start index
    :param num_items: Number of items
    :return: dictionary of tables to write to
    """
    
    serializer = boto3.dynamodb.types.TypeSerializer()
    item_batch = { table_name: []}
    item_batch_list = items[start_num : start_num + batch_size]
    if len(item_batch_list) < 1:
        return None
    for item in item_batch_list:
        item_serialized = {k: serializer.serialize(v) for k,v in item.items()}
        item_batch[table_name].append({'PutRequest': {'Item': item_serialized}})
                
    return item_batch

def prepare_matches_item_list(matches):
    """Make a list of matches to retrieve from ddb."""
    item_list = []
    for match in matches:
        item_list.append({"pk": "match", "sk": str(match) + "1"})
        item_list.append({"pk": "match", "sk": str(match) + "2"})
    return item_list

def prepare_stats_item_list(matches, pk):
    """Make a list of stats or wstats to retrieve from ddb."""
    item_list = []
    for match in matches:
        item_list.append({"pk": pk, "sk": str(match)})
    return item_list

def ddb_batch_write(client, table_name, items):
        messages = ""
        num_items = len(items)
        logger.info(f'Performing ddb_batch_write to dynamo with {num_items} items.')
        start = 0
        batch_size = 25
        while True:
            # Loop adding 25 items to dynamo at a time
            request_items = create_batch_write_structure(table_name,items, start, batch_size)
            if not request_items:
                break
            try: 
                response = client.batch_write_item(RequestItems=request_items)
            except ClientError as err:
                logger.error(err.response['Error']['Message'])
                logger.error("Failed to run full batch_write_item")
                raise
            if len(response['UnprocessedItems']) == 0:
                logger.info(f'Wrote a batch of about {batch_size} items to dynamo')
            else:
                # Hit the provisioned write limit
                logger.warning('Hit write limit, backing off then retrying')
                sleep_time = 5 #seconds
                logger.warning(f"Sleeping for {sleep_time} seconds")
                _time.sleep(sleep_time)

                # Items left over that haven't been inserted
                unprocessed_items = response['UnprocessedItems']
                logger.warning('Resubmitting items')
                # Loop until unprocessed items are written
                while len(unprocessed_items) > 0:
                    response = client.batch_write_item(RequestItems=unprocessed_items)
                    # If any items are still left over, add them to the
                    # list to be written
                    unprocessed_items = response['UnprocessedItems']

                    # If there are items left over, we could do with
                    # sleeping some more
                    if len(unprocessed_items) > 0:
                        sleep_time = 5 #seconds
                        logger.warning(f"Sleeping for {sleep_time} seconds")
                        _time.sleep(sleep_time)

                # Inserted all the unprocessed items, exit loop
                logger.warning('Unprocessed items successfully inserted')
                break
            if response["ResponseMetadata"]['HTTPStatusCode'] != 200:
                messages += f"\nBatch {start} returned non 200 code"
            start += 25
            
# print(matchinfo["match_id"].ljust(12) + matchinfo["round"].ljust(2) + matchinfo["map"].ljust(20) + matchinfo["winner"].ljust(10))