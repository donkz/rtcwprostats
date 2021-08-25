import boto3
import logging
import json
import time
from datetime import datetime
import botocore
from botocore.exceptions import ClientError
from collections import Counter

ddb_client = boto3.client('dynamodb')
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # set to DEBUG for verbose boto output
logging.basicConfig(level = logging.INFO)

def ddb_get_item(pk, sk, table):
    result = None
    try:
        response = table.get_item(Key={'pk': pk, 'sk': sk})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        raise
    else:
        if "Item" in response:    
            result = response['Item']
    return result

def ddb_get_server(sk, table):
    """
    Parameters
    ----------
    sk : string
        server identifier (name).
    table : ddb table

    Returns
    -------
    result : Will return server json or None

    """
    result = None
    try:
        response = table.get_item(Key={'pk': "server", 'sk': sk})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        raise
    else:
        if "Item" in response:    
            result = response['Item']
    return result

def ddb_update_item(key, expression, values, table):
    try: 
        response = table.update_item(Key=key, UpdateExpression=expression,ExpressionAttributeValues=values, ExpressionAttributeNames={"#data_value": "data"})
    except botocore.exceptions.ClientError as err:
        logger.error(err.response['Error']['Message'])
        logger.error("Item was: " + str(key))
        #raise
    else:
        pk = key["pk"]
        sk = key["sk"]
        http_code = "No http code"
        
        try: 
            http_code = response['ResponseMetadata']['HTTPStatusCode']
        except: 
            http_code = "Could not retrieve http code"
            logger.error("Could not retrieve http code from response\n" + str(response))
        
        if http_code != 200:
            logger.error(f"Erroneous HTTP Code ({http_code}) while updating an item \n" + str(key) + "\n" + str(response))

def ddb_update_server_record(gamestats, table, region, date_time_human):
    key = {
        'pk'    : "server",
        'sk'    : gamestats['serverinfo']['serverName']
        }
    expression = 'SET submissions = submissions + :val1, lsipk = :val2, #data_value = :val3'
    values = {':val1': 1, ':val2': region + "#" + date_time_human, ':val3': gamestats["serverinfo"]}
    ddb_update_item(key, expression, values, table)

def ddb_prepare_server_item(gamestats):
    region = None
    server_name = gamestats['serverinfo']['serverName']
    
    if " na "       in server_name.lower(): region = 'na'
    if "-na "       in server_name.lower(): region = 'na'
    if " virginia " in server_name.lower(): region = 'na'
    if " donkz "    in server_name.lower(): region = 'na'
    if " eu "       in server_name.lower(): region = 'eu'
    if "-eu "       in server_name.lower(): region = 'eu'
    if "adlad"      in server_name.lower(): region = 'eu'
    if "amster"     in server_name.lower(): region = 'eu'
    if "london"     in server_name.lower(): region = 'eu'
    if " sa "       in server_name.lower(): region = 'sa'
    if "chile"      in server_name.lower(): region = 'sa'
    if "brazil"     in server_name.lower(): region = 'sa'
    
    if not region: region = 'unk'
    
    
    server_item = {
        'pk'    : 'server',
        'sk'    : server_name,
        'lsipk' : region + '#' + gamestats["gameinfo"]["date_time_human"],
        'data'  : gamestats["serverinfo"],
        'submissions' : 1,
        'region' : region
        }
    return server_item

def ddb_put_item(Item, table):
    try: 
        response = table.put_item(Item=Item)
    except botocore.exceptions.ClientError as err:
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

def ddb_prepare_match_item(gamestats):
    inject_json_version(gamestats["gameinfo"], gamestats)
    match_item = {
        'pk'    : 'match',
        'sk'    : gamestats["gameinfo"]["match_id"] + gamestats["gameinfo"]["round"],
        'lsipk' : gamestats["match_type"] + "#" + gamestats["gameinfo"]["match_id"] + gamestats["gameinfo"]["round"],
        'gsi1pk': "match",
        'gsi1sk': gamestats['serverinfo']['serverName'] + "#" + gamestats["gameinfo"]["match_id"] + gamestats["gameinfo"]["round"],
        'data'  : json.dumps(gamestats["gameinfo"])
        }
    return match_item

def ddb_prepare_stats_items(gamestats):
    
    #debug
    #playerguid = list(gamestats["stats"][0].keys())[0] 
    #stat = gamestats["stats"][0][playerguid]
    
    #this is some stupid fix ddb requires
    #nevermind... saving everything as strings
    # https://github.com/boto/boto3/issues/665
    #from decimal import Decimal
    #stat["categories"]["accuracy"] = Decimal(str(stat["categories"]["accuracy"]))
    #stat["categories"]["efficiency"] = Decimal(str(stat["categories"]["efficiency"]))
    
    stats_items = []
    duplicates_check = {}
    matchid = gamestats["gameinfo"]["match_id"]
    
    tmp_stats_unnested = fix_stats_nesting(gamestats)
    for player_item in tmp_stats_unnested:
        for playerguid, stat in player_item.items():
            inject_json_version(stat, gamestats)
            stats_item = {
                'pk'    : 'stats#' + playerguid,
                'sk'    : matchid,
                'gsi1pk': "stats#" + gamestats["match_type"],
                'gsi1sk': matchid,
                'data'  : json.dumps(stat)
             }
            if playerguid in duplicates_check:
                logger.warning("Skipping duplicate player in stats " + playerguid)
            else:
                stats_items.append(stats_item)
                duplicates_check[playerguid]=1
                   
    logger.info("Number of players in stats_items: " + str(len(stats_items)))
    return stats_items

def ddb_prepare_statsall_item(gamestats):
    inject_json_version(gamestats["stats"], gamestats)
    statsall_item = {
        'pk'    : 'statsall',
        'sk'    : gamestats["gameinfo"]["match_id"],
        'gsi1pk': "statsall#" + gamestats["match_type"],
        'gsi1sk': gamestats["gameinfo"]["match_id"],
        'data'  : json.dumps(gamestats["stats"])
        }
    return statsall_item

def ddb_prepare_gamelog_item(gamestats):
    gamelog_item = {
        'pk'    : 'gamelogs',
        'sk'    : gamestats["gameinfo"]["match_id"] + gamestats["gameinfo"]["round"],
        'data'  : json.dumps(gamestats["gamelog"])
        }
    return gamelog_item

def ddb_prepare_wstat_items_obsolete(gamestats):
    player_guids = Counter()
    wstat_items = []
    matchid = gamestats["gameinfo"]["match_id"]
    for player in gamestats['wstats']:
        playerguid = list(player.keys())[0] #this could be fixed 
        if player_guids[playerguid] > 0:
            continue
        player_guids[playerguid] +=1
        for wstat in player[playerguid]:
            inject_json_version(wstat, gamestats)
            wstat_item = {
                'pk'    : "wstats#" + playerguid,
                'sk'    : wstat["weapon"] + "#" + matchid,
                'data'  : json.dumps(wstat)
                }
            wstat_items.append(wstat_item)
    return wstat_items

def ddb_prepare_wstat_items(gamestats):
    player_guids = Counter()
    wstat_items = []
    matchid = gamestats["gameinfo"]["match_id"]
    for player in gamestats['wstats']:
        playerguid = list(player.keys())[0] #this could be fixed 
        if player_guids[playerguid] > 0: #in January 2021 some players were repeating
            continue
        player_guids[playerguid] +=1
        #inject_json_version(wstat, gamestats)
        wstat_item = {
            'pk'    : "wstats#" + playerguid,
            'sk'    : matchid,
            'data'  : json.dumps(player[playerguid])
            }
        wstat_items.append(wstat_item)
    return wstat_items

def ddb_prepare_wstatsall_item(gamestats):   
    inject_json_version(gamestats['wstats'], gamestats)
    wstatsall_item ={
            'pk'    : "wstatsall",
            'sk'    : gamestats["gameinfo"]["match_id"],
            'data'  : json.dumps(gamestats['wstats'])
        }
    return wstatsall_item

def ddb_prepare_player_items(gamestats):
    player_items = []
    matchid = gamestats["gameinfo"]["match_id"]
    for playerguid, stat in gamestats["stats"][0].items():
        player_item = {
            'pk'    : 'player',
            'sk'    : "aliases" + "#" + playerguid + "#" + matchid,
            'lsipk' : "recentaliases" + "#" + matchid + "#" + playerguid,
            'data'  : stat["alias"]
            }
        player_items.append(player_item)
    return player_items

def ddb_prepare_log_item(match_id_rnd,file_key,
                         match_item_size, 
                         num_stats_items, 
                         statsall_item_size,
                         gamelog_item_size,
                         num_wstats_items,
                         wstatsall_item_size,
                         num_player_items,
                         #timestamp,
                         submitter_ip):    
    log_item ={
            'pk'            : "match",
            'sk'            : "log#" + match_id_rnd + "#" + file_key,
            'lsipk'         : "log#" + file_key + "#" + match_id_rnd,
            'match_size'    : match_item_size, 
            'stats_num'     : num_stats_items, 
            'statsall_size' : statsall_item_size,
            'gamelog_size'  : gamelog_item_size,
            'num_wstats'    : num_wstats_items,
            'wstatsall_size': wstatsall_item_size,
            'players_num'   : num_player_items,
            'timestamp'     : datetime.now().isoformat(),
            'submitter_ip'  : submitter_ip
        }
    return log_item


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
        message = ""
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
            except botocore.exceptions.ClientError as err:
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
                time.sleep(sleep_time)

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
                        time.sleep(sleep_time)

                # Inserted all the unprocessed items, exit loop
                logger.warning('Unprocessed items successfully inserted')
                break
            if response["ResponseMetadata"]['HTTPStatusCode'] != 200:
                message += f"\nBatch {start} returned non 200 code"
                logger.warning(message)
            start += 25

def inject_json_version(obj, gamestats):
    if isinstance(obj, list):
        logger.info("Skipping list while inserting the versions")  # TODO
    elif isinstance(obj, dict):  
        obj['jsonGameStatVersion'] = gamestats["serverinfo"]["jsonGameStatVersion"]
    else:
        logger.warning("Unidentified stats object!")

def fix_stats_nesting(gamestats):
    stats_new_object = []
    if len(gamestats["stats"]) == 2:
            logger.info("Number of items in stats: 2")
            for k,v in gamestats["stats"][0].items():
                stats_new_object.append({k:v})
            for k,v in gamestats["stats"][1].items():
                stats_new_object.append({k:v})   
            logger.info("New statsall has " + str(len(stats_new_object)) + " players")
    else:
        stats_new_object = gamestats["stats"]
    return stats_new_object

'''
CLI Tests
#########################
match retrieval         #
#########################

$ aws dynamodb get-item --table-name rtcwprostats --key '{"pk": {"S": "match"},"sk": {"S": "16098263842"}}' --return-consumed-capacity TOTAL
{
    "Item": {
        "lsipk": {
            "S": "na#g616098263842"
        },
        "sk": {
            "S": "16098263842"
        },
        "gsi1sk": {
            "S": "16098263842"
        },
        "pk": {
            "S": "match"
        },
        "data": {
            "S": "{'match_id': '1609826384', 'round': '2', 'round_start': '1609826657', 'round_end': '1609826799', 'map': 'te_delivery_b1', 'time_limit': '3:54', 'allies_cycle': '20', 'axis_cycle': '30', 'winner': 'Allied'}"
        },
        "gsi1pk": {
            "S": "match#te_delivery_b1"
        }
    },
    "ConsumedCapacity": {
        "TableName": "rtcwprostats",
        "CapacityUnits": 0.5
    }
}

#########################
stat  retrieval         #
#########################

$ aws dynamodb get-item --table-name rtcwprostats --key '{"pk": {"S": "stats#980A27C4F9E48F"},"sk": {"S": "1609826384"}}' --return-consumed-capacity TOTAL
{
    "Item": {
        "sk": {
            "S": "1609826384"
        },
        "gsi1sk": {
            "S": "1609826384"
        },
        "pk": {
            "S": "stats#980A27C4F9E48F"
        },
        "data": {
            "S": "{'alias': 'pipe', 'team': 'Axis', 'start_time': 15256400, 'num_rounds': 2, 'categories': {'kills': 4, 'deaths': 7, 'gibs': 1, 'suicides': 1, 'teamkills': 1, 'headshots': 3, 'damagegiven': 910, 'damagereceived': 1283, 'damageteam': 163, 'hits': 34, 'shots': 129, 'accuracy': Decimal('26.356589147286822'), 'revives': 1, 'ammogiven': 0, 'healthgiven': 0, 'poisoned': 0, 'knifekills': 0, 'killpeak': 1, 'efficiency': Decimal('36.0'), 'score': 6, 'dyn_planted': 11, 'dyn_defused': 0, 'obj_captured': 0, 'obj_destroyed': 0, 'obj_returned': 0, 'obj_taken': 4}}"
        },
        "gsi1pk": {
            "S": "stats#na#g6"
        }
    },
    "ConsumedCapacity": {
        "TableName": "rtcwprostats",
        "CapacityUnits": 0.5
    }
}

#########################
statsall  retrieval     #
#########################
$ aws dynamodb get-item --table-name rtcwprostats --key '{"pk": {"S": "statsall"},"sk": {"S": "1609826384"}}' --return-consumed-capacity TOTAL
{
    "Item": {
        "lsipk": {
            "S": "980A27C4F9E48F"
        },
        "sk": {
            "S": "1609826384"
        },
        "gsi1sk": {
            "S": "1609826384"
        },
        "pk": {
            "S": "statsall"
        },
        "data": {
            "S": "[{'980A27C4F9E48F': {'alias': 'pipe', 'team': 'Axis', 'start_time': 15256400, 'num_rounds': 2, 'categories': {'kills': 4, 'deaths': 7, 'gibs': 1, 'suicides': 1, 'teamkills': 1, 'headshots': 3, 'damagegiven': 910, 'damagereceived': 1283, 'damageteam': 163, 'hits': 34, 'shots': 129, 'accuracy': Decimal('26.356589147286822'), 'revives': 1, 'ammogiven': 0, 'healthgiven': 0, 'poisoned': 0, 'knifekills': 0, 'killpeak': 1, 'efficiency': Decimal('36.0'), 'score': 6, 'dyn_planted': 11, 'dyn_defused': 0, 'obj_captured': 0, 'obj_destroyed': 0, 'obj_returned': 0, 'obj_taken': 4}}, '5F27C60BA65823': {'alias': 'vriuk', 'team': 'Axis', 'start_time': 15256400, 'num_rounds': 2, 'categories': {'kills': 7, 'deaths': 9, 'gibs': 1, 'suicides': 0, 'teamkills': 0, 'headshots': 6, 'damagegiven': 994, 'damagereceived': 1185, 'damageteam': 50, 'hits': 53, 'shots': 229, 'accuracy': 23.14410480349345, 'revives': 1, 'ammogiven': 8, 'healthgiven': 0, 'poisoned': 0, 'knifekills': 0, 'killpeak': 1, 'efficiency': 43.0, 'score': 4, 'dyn_planted': 0, 'dyn_defused': 0, 'obj_captured': 0, 'obj_destroyed': 0, 'obj_returned': 0, 'obj_taken': 2}}, 'D6809D025CD614': {'alias': 'resiak', 'team': 'Axis', 'start_time': 15256400, 'num_rounds': 2, 'categories': {'kills': 5, 'deaths': 7, 'gibs': 1, 'suicides': 1, 'teamkills': 0, 'headshots': 5, 'damagegiven': 813, 'damagereceived': 1279, 'damageteam': 69, 'hits': 25, 'shots': 127, 'accuracy': 19.68503937007874, 'revives': 2, 'ammogiven': 0, 'healthgiven': 0, 'poisoned': 0, 'knifekills': 0, 'killpeak': 1, 'efficiency': 41.0, 'score': -2, 'dyn_planted': 0, 'dyn_defused': 0, 'obj_captured': 0, 'obj_destroyed': 0, 'obj_returned': 0, 'obj_taken': 3}}, 'A53B3ED2A896CB': {'alias': 'parcher', 'team': 'Axis', 'start_time': 15256400, 'num_rounds': 2, 'categories': {'kills': 18, 'deaths': 12, 'gibs': 3, 'suicides': 5, 'teamkills': 0, 'headshots': 23, 'damagegiven': 3308, 'damagereceived': 2191, 'damageteam': 0, 'hits': 157, 'shots': 323, 'accuracy': 48.60681114551084, 'revives': 0, 'ammogiven': 0, 'healthgiven': 0, 'poisoned': 0, 'knifekills': 0, 'killpeak': 2, 'efficiency': 60.0, 'score': -3, 'dyn_planted': 3, 'dyn_defused': 0, 'obj_captured': 0, 'obj_destroyed': 0, 'obj_returned': 5, 'obj_taken': 4}}}, {'E79B8A18A9DBB7': {'alias': 'john_mullins', 'team': 'Allied', 'start_time': 15256400, 'num_rounds': 2, 'categories': {'kills': 13, 'deaths': 7, 'gibs': 1, 'suicides': 6, 'teamkills': 1, 'headshots': 11, 'damagegiven': 2546, 'damagereceived': 1996, 'damageteam': 118, 'hits': 117, 'shots': 325, 'accuracy': 36.0, 'revives': 2, 'ammogiven': 0, 'healthgiven': 0, 'poisoned': 0, 'knifekills': 1, 'killpeak': 3, 'efficiency': 65.0, 'score': 28, 'dyn_planted': 2, 'dyn_defused': 0, 'obj_captured': 0, 'obj_destroyed': 0, 'obj_returned': 1, 'obj_taken': 5}}, '44D0B08B78E9F2': {'alias': 'c@k-el', 'team': 'Allied', 'start_time': 15256400, 'num_rounds': 2, 'categories': {'kills': 5, 'deaths': 9, 'gibs': 2, 'suicides': 0, 'teamkills': 1, 'headshots': 0, 'damagegiven': 536, 'damagereceived': 1435, 'damageteam': 168, 'hits': 27, 'shots': 149, 'accuracy': 18.120805369127517, 'revives': 4, 'ammogiven': 0, 'healthgiven': 4, 'poisoned': 0, 'knifekills': 1, 'killpeak': 2, 'efficiency': 35.0, 'score': 9, 'dyn_planted': 2, 'dyn_defused': 0, 'obj_captured': 0, 'obj_destroyed': 0, 'obj_returned': 3, 'obj_taken': 2}}, '66A2121F6337E8': {'alias': 'blazk', 'team': 'Allied', 'start_time': 15256400, 'num_rounds': 2, 'categories': {'kills': 2, 'deaths': 11, 'gibs': 4, 'suicides': 0, 'teamkills': 0, 'headshots': 3, 'damagegiven': 708, 'damagereceived': 1034, 'damageteam': 0, 'hits': 38, 'shots': 124, 'accuracy': 30.64516129032258, 'revives': 0, 'ammogiven': 9, 'healthgiven': 0, 'poisoned': 0, 'knifekills': 0, 'killpeak': 1, 'efficiency': 15.0, 'score': 6, 'dyn_planted': 7, 'dyn_defused': 0, 'obj_captured': 0, 'obj_destroyed': 0, 'obj_returned': 2, 'obj_taken': 3}}, 'FDE32828995C0A': {'alias': 'fonze(s)', 'team': 'Allied', 'start_time': 15256400, 'num_rounds': 2, 'categories': {'kills': 14, 'deaths': 9, 'gibs': 2, 'suicides': 0, 'teamkills': 0, 'headshots': 13, 'damagegiven': 2148, 'damagereceived': 1560, 'damageteam': 36, 'hits': 74, 'shots': 280, 'accuracy': 26.428571428571427, 'revives': 2, 'ammogiven': 0, 'healthgiven': 0, 'poisoned': 0, 'knifekills': 1, 'killpeak': 1, 'efficiency': 60.0, 'score': 1, 'dyn_planted': 0, 'dyn_defused': 1, 'obj_captured': 0, 'obj_destroyed': 0, 'obj_returned': 2, 'obj_taken': 4}}}]"
        },
        "gsi1pk": {
            "S": "statsall#na#g6"
        }
    },
    "ConsumedCapacity": {
        "TableName": "rtcwprostats",
        "CapacityUnits": 1.0
    }
}

#########################
gamelog  retrieval      #
#########################
$ aws dynamodb get-item --table-name rtcwprostats --key '{"pk": {"S": "gamelog"},"sk": {"S": "16098263842"}}' --return-consumed-capacity TOTAL
"CapacityUnits": 1.5
'''