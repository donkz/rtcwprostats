try:
    from gamelog_process.longest_kill import LongestKill
    from gamelog_process.frontliner import Frontliner
    from gamelog_process.megakill import MegaKill
    from gamelog_process.top_feuds import TopFeuds
except:
    print("Debug imports")
    from longest_kill import LongestKill
    from frontliner import Frontliner
    from megakill import MegaKill
    from top_feuds import TopFeuds

import logging
from botocore.exceptions import ClientError
import json
import boto3
from boto3.dynamodb.conditions import Key
import math
from datetime import datetime
import time as _time

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("gamelog_calc")
logger.setLevel(log_level)


def process_gamelog(ddb_table, ddb_client, match_or_group_id, log_stream_name):
    """Main logic for processing a collection of gamelogs."""
    # Put individual award calculator classes into an array 
    award_classes = [
        LongestKill(),
        Frontliner(),
        MegaKill(),
        TopFeuds()
        ]
    achievment_award_names = ["Longest Kill", "MegaKill"]
    
    is_single_match = isinstance(match_or_group_id, int)
    is_group = not is_single_match

    gamelog_all, match_region_type = get_multi_round_gamelog_array(ddb_table, match_or_group_id,log_stream_name, is_single_match)
    
    # Loop through all events in the match or a group
    for rtcw_event in gamelog_all:
        # feed each event to a different award calculator class
        for class_ in award_classes:
            class_.process_event(rtcw_event)
    
    potential_achievements = {}
    awards = {}
    top_feuds = []
    for class_ in award_classes:
        if is_single_match and class_.award_name in achievment_award_names:
            potential_achievements.update(class_.get_full_results())
        if is_group:
            if isinstance(class_, TopFeuds):
                try:
                    top_feuds = class_.get_custom_results()
                except Exception as ex:
                    template = "Top Feuds: An exception of type {0} occurred. Arguments:\n{1!r}"
                    error_msg = template.format(type(ex).__name__, ex.args)
                    logger.error(error_msg)
            else:   
                awards.update(class_.get_all_top_results())

    # only perform achievements per match (round 2)
    # by the time group is created, all personal achievements had been processed
    if is_single_match:
        logger.info("Updating achievements for a single match.")
        real_names = get_real_names(potential_achievements, [],  ddb_table)
        update_achievements(ddb_table, ddb_client, potential_achievements, log_stream_name, real_names, match_region_type)
    
    if is_group:
        logger.info("Saving cache for a group of matches.")
        real_names = get_real_names(awards, top_feuds, ddb_table)
        cache_group_awards(ddb_table, awards, top_feuds, match_or_group_id, real_names)


def cache_group_awards(ddb_table, awards, top_feuds, match_or_group_id, real_names):
    """Cache results of the top awards for groups."""
    
    awards_with_names = {}
    for award, award_table in awards.items():
        awards_with_names[award] = {}
        for guid in award_table:
            real_name = real_names.get(guid, "no_name")
            awards_with_names[award][real_name] = award_table[guid]
    
    feuds_with_names = []
    for feud in top_feuds:
        # ['a928daaba6dbdb67ba9b392c3966b22e', '75955f0e5c3daa77a2e9250357195dbf', 3, 0]
        feuds_with_names.append([
            real_names.get(feud[0], "no_name"),
            real_names.get(feud[1], "no_name"),
            feud[2],
            feud[3]
            ]
            )
    
    item = {
        'pk': "groupawards",
        'sk': match_or_group_id,
        'data': awards_with_names,
        'top_feuds': feuds_with_names
    }
    ddb_put_item(item, ddb_table)
    logger.info("Cached group awards under " + "pk: groupawards"+ " sk: " + match_or_group_id)


def get_multi_round_gamelog_array(ddb_table, match_or_group_id, log_stream_name, is_single_match):
    """Based on the list of matches get their gamelogs for both rounds."""
    matches = []
    match_region_type = "na#6"
    if is_single_match:
        matches.append(match_or_group_id)
        match_response = ddb_table.query(KeyConditionExpression=Key("pk").eq("match") & Key("sk").begins_with(str(match_or_group_id)), Limit=1, ScanIndexForward=False)
        if len(match_response.get("Items",[])) > 0:
            match_region_type = "#".join(match_response["Items"][0]["lsipk"].split("#")[0:2])
    else:
        group_response = ddb_table.query(KeyConditionExpression=Key("pk").eq("group") & Key("sk").begins_with(match_or_group_id), Limit=1, ScanIndexForward=False)
        if len(group_response.get("Items",[])) > 0:
            matches = json.loads(group_response["Items"][0]["data"])
            match_region_type = "#".join(group_response["Items"][0]["lsipk"].split("#")[0:2])
            
    big_item_list = []
    for match_id in matches: 
        big_item_list.append({"pk": "gamelogs", "sk": str(match_id) + "1"})
        big_item_list.append({"pk": "gamelogs", "sk": str(match_id) + "2"})
    
    logger.info("Getting gamelogs for " + str(len(big_item_list)) + " matches.")
    gamelog_responses = get_big_batch_items(big_item_list, ddb_table, log_stream_name)
    
    gamelog_all = []
    for gamelog_item in gamelog_responses:
        gamelog_all.extend(json.loads(gamelog_item["data"]))
    
    return gamelog_all, match_region_type


def update_achievements(ddb_table, ddb_client, potential_achievements, log_stream_name, real_names, match_region_type):
    """Update personal achievments for each player."""
    big_item_list = []
    
    for award, award_table in potential_achievements.items():
        for guid in award_table:
            big_item_list.append({"pk": "player#" + guid, "sk": "achievement#" + award + "#" + match_region_type})
    
    logger.info("Getting achievements for " + str(len(big_item_list)) + " values.")
    achievments_old_response = get_big_batch_items(big_item_list, ddb_table, log_stream_name)
    logger.info("Got achievements for " + str(len(achievments_old_response)) + " values.")
    
    achievments_old = {}
    for achievement_item in achievments_old_response:
        guid = achievement_item["pk"].split("#")[1]
        achievement_code = achievement_item["sk"].split("#")[1]
        achievments_old[guid + "#" + achievement_code] = float(achievement_item["gsi1sk"])
    
    update_achievement_items = ddb_prepare_achievement_items(potential_achievements, achievments_old, real_names, match_region_type)
    
    # submit updated summaries
    items = []
    items.extend(update_achievement_items)
    
    if len(items) > 0:
        try:
            ddb_batch_write(ddb_client, ddb_table.name, items)
        except Exception as ex:
            template = "gamelog_calc.ddb_batch_write: An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            message = "Failed to insert achievements for a match.\n" + error_msg
        else:
            message = "Achievements records inserted."
    else:
        message = "No achievements to insert this time."
    logger.info(message)
    
    
def make_error_dict(message, item_info):
    """Make an error message for API gateway."""
    return {"error": message + " " + item_info}


def get_batch_items(item_list, ddb_table, log_stream_name):
    """Get items in a batch."""
    dynamodb = boto3.resource('dynamodb')
    item_info = "get_batch_items. Logstream: " + log_stream_name
    try:
        response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list, 'ProjectionExpression': 'pk, sk, #data_value, gsi1pk, gsi1sk', 'ExpressionAttributeNames': {'#data_value': 'data'} }}, ReturnConsumedCapacity='NONE')
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if len(response["Responses"][ddb_table.name]) > 0:
            result = response["Responses"][ddb_table.name]
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result


def get_big_batch_items(big_item_list, ddb_table, log_stream_name):
    """Get over 100 batch items."""
    
    num_items = len(big_item_list)
    
    start = 0
    batch_size = 100
    
    batches = math.ceil(num_items/batch_size) 
    logger.info(f'Performing get_batch_items from dynamo with {num_items} items in {batches} batches.')
    
    item_list_list = []
    for i in range(1, batches+1):
        item_list_list.append(big_item_list[start: start + batch_size])
        start += batch_size
    
    big_response = []
    for item_list in item_list_list:
        response = get_batch_items(item_list, ddb_table, log_stream_name)
        if "error" not in response:
            big_response.extend(response)
    
    return big_response


def ddb_prepare_achievement_items(potential_achievements, achievments_old, real_names, match_region_type):
    items = []
    ts = datetime.now().isoformat()
    for achievement, achievement_table in potential_achievements.items():
        for guid, player_value in achievement_table.items():
            if int(player_value) > int(achievments_old.get(guid + "#" + achievement,0)):
                item = {
                    'pk'            : "player"+ "#" + guid,
                    'sk'            : "achievement#" + achievement + "#" + match_region_type,
                    'lsipk'         : "achievement#" + ts,
                    'gsi1pk'        : "leader#" + achievement + "#" + match_region_type,
                    'gsi1sk'        : str(player_value).zfill(6),
                    "real_name"     : real_names.get(guid, "no_name#")
                }
                items.append(item)
    return items


def get_real_names(potential_achievements, top_feuds, ddb_table):
    real_name_item_list = prepare_playerinfo_list(potential_achievements, top_feuds, "realname")
    response = get_batch_items(real_name_item_list, ddb_table, "real_names")
    real_names = {}
    if "error" not in response:
        for result in response:
            guid = result["pk"].split("#")[1]
            real_names[guid] = result["data"]
    return real_names

            
def prepare_playerinfo_list(potential_achievements, top_feuds, sk):
    """Make a list of guids to retrieve from ddb."""
    item_list = []
    unique_guids = {}
    for achievement, achievement_table in potential_achievements.items(): 
        for guid, player_stats in achievement_table.items():
            if {"pk": "player#" + guid, "sk": sk} not in item_list:
                item_list.append({"pk": "player#" + guid, "sk": sk})
                unique_guids[guid] = 1
    
    for feud in top_feuds:
        if feud[0] not in unique_guids:
            item_list.append({"pk": "player#" + feud[0], "sk": sk})
            unique_guids[feud[0]] = 1
        if feud[1] not in unique_guids:
            item_list.append({"pk": "player#" + feud[1], "sk": sk})
            unique_guids[feud[1]] = 1
    return item_list


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
            
def ddb_put_item(Item, table):
    """Put a single item in ddb."""
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