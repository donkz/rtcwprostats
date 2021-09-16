import logging
from botocore.exceptions import ClientError
import json
import boto3
import time as _time

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("summary_calc")
logger.setLevel(log_level)
  
def process_rtcwpro_summary(ddb_table, ddb_client, match_id, log_stream_name):
    "RTCWPro pipeline specific logic."
    t1 = _time.time()
    sk = match_id
    message = ""
    
    response = get_item("statsall", sk, ddb_table, log_stream_name)
    if "error" not in response:
        stats = json.loads(response["data"])
        match_region_type = response['gsi1pk'].replace("statsall#","")
        logger.info("Retrieved statsall for " + str(len(stats)) + " players")
        stats = convert_stats_to_dict(stats)
    else:
        logger.error("Failed to retrieve statsall: " + sk)
        logger.error(json.dumps(response))
        message += "Error in getting stats" + response["error"]
        return message
    
    response = get_item("wstatsall", sk, ddb_table, log_stream_name)
    if "error" not in response:
        wstats = json.loads(response["data"])
        logger.info("Retrieved wstatsall for " + str(len(wstats)) + " players")
    else:
        logger.error("Failed to retrieve wstatsall: " + sk)
        logger.error(json.dumps(response))
        message += "Error in getting wstats" + response["error"]
        return message
    
    
    real_name_list = prepare_playerinfo_list(stats, match_region_type, "realname")
    item_list = real_name_list
    response = get_batch_items(item_list, ddb_table, log_stream_name)
    
    real_names = {}
    if "error" not in response:
        for result in response:
            guid = result["pk"].split("#")[1]
            if "data" in result:
                real_names[guid] = result["data"]
                logger.debug("Retrieved " + guid + " name: " + result["data"])
    
    aggstats_item_list = prepare_playerinfo_list(stats, match_region_type, "aggstats#" + match_region_type)
    item_list = aggstats_item_list
    response = get_batch_items(item_list, ddb_table, log_stream_name)

    stats_old = {}
    if "error" not in response:
        for result in response:
            guid = result["pk"].split("#")[1]
            if "data" in result:
                stats_old[guid] = result["data"]
    else:
        if "Items do not exist" in response["error"]:
            logger.warning("Starting fresh.")
        else:   
            logger.error("Failed to retrieve any player wstats.")
            logger.error(json.dumps(response))
            message += "Error in getting prepare_playerinfo_list" + response["error"]
            return message
    
    aggwstats_item_list = prepare_playerinfo_list(stats, match_region_type, "aggwstats#" + match_region_type)
    item_list = aggwstats_item_list
    response = get_batch_items(item_list, ddb_table, log_stream_name)

    wstats_old = {}
    if "error" not in response:
        for result in response:
            guid = result["pk"].split("#")[1]
            if "data" in result:
                wstats_old[guid] = result["data"]
    else:
        if "Items do not exist" in response["error"]:
            logger.warning("Starting fresh.")
        else:   
            logger.error("Failed to retrieve any player wstats.")
            logger.error(json.dumps(response))
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
    stats_items = ddb_prepare_statswstats_items("stats", stats_dict_updated, match_region_type, real_names)
    
    # build updated wtats summaries
    wstats = new_wstats
    wstats_dict_updated = build_new_wstats_summary(wstats, wstats_old)
    wstats_items = ddb_prepare_statswstats_items("wstats", wstats_dict_updated, match_region_type, real_names)

    # submit updated summaries
    items = []
    items.extend(stats_items)
    items.extend(wstats_items)
    
    try:
        ddb_batch_write(ddb_client, ddb_table.name, items)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to load aggregate stats for a match " + match_id + "\n" + error_msg
        logger.info(message)
    else:
        message = "Elo progress records inserted.\n"
    
    time_to_write = str(round((_time.time() - t1), 3))
    logger.info(f"Time to process summaries is {time_to_write} s")
    message += "Records were summarized"
    return message

def convert_stats_to_dict(stats):
    if len(stats) == 2 and len(stats[0]) > 1: #stats grouped in teams in a list of 2 teams , each team over 1 player
        logger.info("Number of stats entries 2, trying to merge teams")
        stats_tmp = stats[0].copy()
        stats_tmp.update(stats[1])
    else:
        logger.info("Merging list into dict.")
        stats_tmp = {}
        for player in stats:
            stats_tmp.update(player)
    logger.info("New statsall has " + str(len(stats_tmp)) + " players in a " + str(type(stats_tmp)))
    return stats_tmp

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


def prepare_playerinfo_list(stats, match_region_type, sk):
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
        response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list, 'ProjectionExpression': 'pk, #data_value', 'ExpressionAttributeNames': {'#data_value': 'data'}}})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if len(response["Responses"][ddb_table.name]) > 0:
            result = response["Responses"][ddb_table.name]
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result


def update_player_info_stats(ddb_table, stats_dict_updated, stats_type):
    """Does not work because missing keys like "na#6#elo throw errors."""
    # Example: ddb_table.update_item(Key=Key, UpdateExpression="set elos.#eloname = :elo, elos.#gamesname = :games", ExpressionAttributeNames={"#eloname": "na#6#elo", "#gamesname": "na#6#games"}, ExpressionAttributeValues={':elo': 134, ':games' : 135})
    update_expression="set " + stats_type + " = :stat"
    for guid, new_stat in stats_dict_updated.items():
        logger.info("Updating player " + stats_type + ": " + guid)
        key = { "pk": "player" , "sk": "playerinfo#" + guid }
            
        expression_values = {':stat': new_stat}
        ddb_table.update_item(Key=key, 
                              UpdateExpression=update_expression, 
                              ExpressionAttributeValues=expression_values)
        
        
def ddb_prepare_statswstats_items(stat_type, dict_, match_region_type, real_names):
    items = []
    for guid, player_stat in dict_.items():
        item = ddb_prepare_stat_item(stat_type, guid, match_region_type, player_stat, real_names)
        items.append(item)
    return items

def ddb_prepare_stat_item(stat_type, guid, match_region_type, player_stat, real_names): 
    if stat_type == "stats":
        sk = "aggstats#" + match_region_type
        gsi1pk = "leaderkdr#" + match_region_type
        try:
            deaths = player_stat["deaths"] if player_stat["deaths"] > 0 else 1
            kdr = player_stat["kills"]/deaths
            games = player_stat["games"]
        except: 
            logger.warning("Could not calculate KDR for guid")
            kdr = 0.0  
            games = 0
        kdr_str = str(round(kdr,1)).zfill(3)
        gsi1sk = kdr_str
        logger.debug("Setting new agg stats for " + guid + " with kdr of " + str(kdr_str))
    elif stat_type == "wstats":
        sk = "aggwstats#" + match_region_type
        gsi1pk = "leaderacc#" + match_region_type
        try:
            acc = calculate_accuracy(player_stat)
            games = player_stat[list(player_stat.keys())[0]]['games']
        except: 
            logger.warning("Could not calculate KDR for guid")
            acc = 0.0
            games = 0
        acc_str = str(round(acc,1)).zfill(4)
        gsi1sk =  acc_str
        logger.debug("Setting new agg stats for " + guid + " with acc of " + str(acc_str))
    
    
    real_name = real_names.get(guid, "no_name#")
    
    item = {
            'pk'            : "player"+ "#" + guid,
            'sk'            : sk,
            # 'lsipk'         : "",
            'gsi1pk'        : gsi1pk,
            'gsi1sk'        : gsi1sk,
            'data'          : player_stat,
            'games'         : games,
            "real_name"     : real_name
        }
    

    return item

def calculate_accuracy(player_stat):
    try:
        hits = shots = 0
        for weapon, wstat in player_stat.items():
            if weapon in ['MP-40','Thompson','Sten', 'Colt', 'Luger']:
                hits += wstat["hits"]
                s = wstat["shots"] if wstat["shots"] > 0 else 1
                shots += s
        acc = round(hits/shots*100, 1)
    except:
        acc = 0.0
    return acc


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