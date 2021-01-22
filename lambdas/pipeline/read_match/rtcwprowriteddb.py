import boto3
import logging
import json

ddb = boto3.client('dynamodb')
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # set to DEBUG for verbose boto output
logging.basicConfig(level = logging.INFO)

def ddb_put_item(Item, table):
    try: 
        response = table.put_item(Item=Item)
    except ddb.exceptions.ClientError as err:
        logger.error(err.response['Error']['Message'])
        logger.error("Item was: " + Item["pk"] + ":" + Item["sk"])
        raise
    return response

def ddb_prepare_match_item(gamestats):
    match_item = {
        'pk'    : 'match',
        'sk'    : gamestats["gameinfo"]["match_id"] + gamestats["gameinfo"]["round"],
        'lsipk' : "na#g6" + "#" + gamestats["gameinfo"]["match_id"] + gamestats["gameinfo"]["round"],
        'gsi1pk': "match#" + gamestats["gameinfo"]["map"],
        'gsi1sk': gamestats["gameinfo"]["match_id"] + gamestats["gameinfo"]["round"],
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
    matchid = gamestats["gameinfo"]["match_id"]
    for team in gamestats["stats"]:
        for playerguid, stat in team.items():
            stats_item = {
                'pk'    : 'stats#' + playerguid,
                'sk'    : matchid,
                'gsi1pk': "stats#" + "na#g6",
                'gsi1sk': matchid,
                'data'  : json.dumps(stat)
             }
            stats_items.append(stats_item)
    return stats_items

def ddb_prepare_statsall_item(gamestats):
    statsall_item = {
        'pk'    : 'statsall',
        'sk'    : gamestats["gameinfo"]["match_id"],
        'gsi1pk': "statsall#" + "na#g6",
        'gsi1sk': gamestats["gameinfo"]["match_id"],
        'data'  : json.dumps(gamestats["stats"])
        }
    return statsall_item

def ddb_prepare_gamelog_item(gamestats):
    gamelog_item = {
        'pk'    : 'gamelog',
        'sk'    : gamestats["gameinfo"]["match_id"] + gamestats["gameinfo"]["round"],
        'data'  : json.dumps(gamestats["gamelog"])
        }
    return gamelog_item

def ddb_prepare_wstat_items(gamestats):

    #debug
    #playerguid = list(gamestats['wstats'][0].keys())[0] #loop
    #wstat = gamestats['wstats'][0][playerguid][0] #loop
    #weapon = wstat["weapon"]
    
    wstat_items = []
    matchid = gamestats["gameinfo"]["match_id"]
    for player in gamestats['wstats']:
        playerguid = list(player.keys())[0] #this could be fixed 
        for wstat in player[playerguid]:
            wstat_item = {
                'pk'    : "wstats#" + playerguid,
                'sk'    : wstat["weapon"] + "#" + matchid,
                'data'  : json.dumps(wstat)
                }
            wstat_items.append(wstat_item)
    return wstat_items

def ddb_prepare_wstatsall_item(gamestats):    
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
            'sk'    : playerguid + "#" + stat["alias"],
            'data'  : matchid
            }
        player_items.append(player_item)
    return player_items





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

