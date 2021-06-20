"""
credit to https://github.com/PredatH0r/XonStat/blob/master/xonstat/elo.py .

ELO algorithm to calculate player ranks

"""
import os
from datetime import datetime
import logging
import math
from botocore.exceptions import ClientError
import json
import boto3
# import random
# import sys
# import pandas as pd
from collections import namedtuple
import time as _time

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("elo_calc")
logger.setLevel(log_level)
debug = False  # very local debug

class PlayerElo(object):
    def __init__(self, player_id=None, game_type_cd=None, elo=None):
        self.create_dt = datetime.utcnow()
        self.player_id = player_id
        self.game_type_cd = game_type_cd
        self.elo = 100.0
        self.g2_r = None
        self.g2_rd = None
        self.score = 0
        self.games = 0
        self.g2_games = 0
        self.g2_dt = None
        self.b_r = None
        self.b_rd = None
        self.b_games = 0
        self.b_dt = None
        
    def __repr__(self):
        return "<PlayerElo(pid=%s, gametype=%s, elo=%s, games=%s)>" % (self.player_id, self.game_type_cd, self.elo, self.games)

    def to_dict(self):
        return {
          'player_id':self.player_id, 'game_type_cd':self.game_type_cd, 'elo':self.elo, 'games':self.games,
          'g2_r':self.g2_r, 'g2_rd':self.g2_rd,  "g2_games":self.g2_games, "g2_dt":self.g2_dt,
          'b_r':self.b_r, 'b_rd':self.b_rd,  "b_games":self.b_games, "b_dt":self.b_dt
        }


class EloParms:
    def __init__(self, global_K = 15, initial = 100, floor = 100, logdistancefactor = math.log(10)/float(400), maxlogdistance = math.log(10)):
        self.global_K = global_K
        self.initial = initial
        self.floor = floor
        self.logdistancefactor = logdistancefactor
        self.maxlogdistance = maxlogdistance


class KReduction:
    #KREDUCTION = KReduction(900, 75, 0.5, 5, 15, 0.2)
    def __init__(self, fulltime, mintime, minratio, games_min, games_max, games_factor):
        self.fulltime = fulltime
        self.mintime = mintime
        self.minratio = minratio
        self.games_min = games_min
        self.games_max = games_max
        self.games_factor = games_factor

    def eval(self, mygames, mytime, matchtime):
        if mytime < self.mintime:
            return 0
        if mytime < self.minratio * matchtime:
            return 0
        if mytime < self.fulltime:
            k = mytime / float(self.fulltime)
        else:
            k = 1.0
        if mygames >= self.games_max:
            k *= self.games_factor
        elif mygames > self.games_min:
            k *= 1.0 - (1.0 - self.games_factor) * (mygames - self.games_min) / float(self.games_max - self.games_min)
        #print("Elo.KReduction.eval: " + str(k))
        return k
  
def process_rtcwpro_elo(ddb_table, ddb_client, match_id, log_stream_name):
    "RTCWPro pipeline specific logic."
    t1 = _time.time()
    error_ecnountered = False
    sk = match_id
    
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
        logger.error("Failed to retrieve statsall.")
        logger.error(json.dumps(response))
        error_ecnountered = True
    
    response = get_item("wstatsall", sk, ddb_table, log_stream_name)
    if "error" not in response:
        wstats = json.loads(response["data"])
        logger.info("Retrieved wstatsall for " + str(len(wstats)) + " players")
    else:
        logger.error("Failed to retrieve wstatsall.")
        logger.error(json.dumps(response))
        error_ecnountered = True

    response = get_item("match", sk + "2", ddb_table, log_stream_name)  # round 2
    if "error" not in response:
        match = json.loads(response["data"])
        match_region = response["lsipk"].split("#")[0]
        match_type = response["lsipk"].split("#")[1]
        match_region_type = match_region + "#" + match_type
        try:
            time_split = match["time_limit"].split(":")
            duration = int(time_split[0]) * 60 + int(time_split[1])
        except Exception:
            duration = 600
        winner = match["winner"]
    else:
        logger.error("Failed to retrieve match.")
        logger.error(json.dumps(response))
        error_ecnountered = True
       
    item_list = prepare_player_elo_list(stats)
    response = get_batch_items(item_list, ddb_table, log_stream_name)

    elo_dict = {}
    elo_games = {}
    old_elos_ddb_maps = {}
    if "error" not in response:
        for result in response:
            if "elos" in result:
                if match_region_type + "#elo" in result["elos"]:
                    guid = result["sk"].split("#")[1]
                    elo_dict[guid] = result["elos"][match_region_type + "#elo"]
                    elo_games[guid] = int(result["elos"].get(match_region_type + "#games",20)) # if elo is there, default to 20
                    old_elos_ddb_maps[guid] = result["elos"]
            try:
                if "real_name" in result:
                    name = result["real_name"]
                else:
                    name = "no_real_name"
                logger.info(result["sk"].split("#")[1] + " " + name.ljust(20) + " elo:" + str(elo_dict[guid]) + " games " + str(elo_games[guid]))
            except:
                logger.warning("Failed to display an elo.")
    else:
        logger.error("Failed to retrieve any player elos.")
        logger.error(json.dumps(response))
        error_ecnountered = True
    
    Player = namedtuple('Player', 'score duration games')
   
    new_wstats = {}
    for wplayer_wrap in wstats:
        for wplayer_guid, wplayer in wplayer_wrap.items():
            new_wplayer = {}
            for weapon in wplayer:
                new_wplayer[weapon["weapon"]] = weapon
            new_wstats[wplayer_guid] = new_wplayer
    
    player_scores = {}
    for guid, player_stats in stats.items():
        score_step_1 = player_stats["categories"].get("kills", 0)
        score_step_2 = score_step_1 \
            - wstat(new_wstats, guid, "Panzer", "kills") * .30 \
            - wstat(new_wstats, guid, "Artillery", "kills") * .30 \
            - wstat(new_wstats, guid, "Airstrike", "kills") * .30 \
            - wstat(new_wstats, guid, "Mauser", "kills") * .30
        
        win_multiplier = 1.5 if player_stats["team"] == winner else 1
        score_step_3 = int(score_step_2 * win_multiplier)
        
        player_scores[guid] = Player(score_step_3, duration, elo_games.get(guid,0)) # if elo is not there, default will be 0 anyway
        if debug:
            print(player_stats.get("alias", "missing_alias").ljust(20) + str(player_stats["categories"].get("kills", 0)).ljust(5) + str(win_multiplier).ljust(5) + str(score_step_3).ljust(5) + str(player_scores[guid].games).ljust(5))

    (elo_deltas, elos) = process_elos(player_scores, elo_dict)
    
    elo_delta_items = ddb_prepare_eloprogress_items(elo_deltas, match_id)
    
    try:
        ddb_batch_write(ddb_client, ddb_table.name, elo_delta_items)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to load all eloprogress records for a match " + match_id + "\n" + error_msg
        logger.info(message)
    else:
        message = "Elo progress records inserted.\n"

    update_player_info(ddb_table, elos, match_region_type, old_elos_ddb_maps)
    
    time_to_write = str(round((_time.time() - t1), 3))
    logger.info(f"Time to process ELOs is {time_to_write} s")
    return message
 
        
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
    

def process_elos(player_scores, elo_dict):
    """Given the players perforance and previous record, calculate new ELOs."""
    duration = player_scores[list(player_scores.keys())[0]].duration
    
    scores = {}
    alivetimes = {}
    games = {}
    
    for guid, p in player_scores.items():
        scores[guid] = p.score
        alivetimes[guid] = p.duration  #quke clan arena only, keeping for code wholesomeness
        games[guid] = p.games
                    
    player_ids = scores.keys()

    elos = {}
    for p in player_ids:
        if p in elo_dict:
            elos[p] = PlayerElo(p)
            elos[p].elo = elo_dict[p]
            elos[p].games = player_scores[p].games
        
    # ensure that all player_ids have an elo record
    for pid in player_ids:
        if pid not in elos.keys():
            elos[pid] = PlayerElo(pid, None, ELOPARMS.initial)

    for pid in list(player_ids):
        elos[pid].k = KREDUCTION.eval(games[pid], alivetimes[pid], duration)
        if elos[pid].k == 0:
            del(elos[pid])
            del(scores[pid])
            del(alivetimes[pid])
    
    elos = update_elos(elos, scores, ELOPARMS)

    return elos


def update_elos(elos, scores, ep):
    if len(elos) < 2:
        return elos

    pids = list(elos.keys())

    eloadjust = {}
    for pid in pids:
        eloadjust[pid] = 0.0

    for i in range(0, len(pids)):
        ei = elos[pids[i]]
        for j in range(i+1, len(pids)):
            ej = elos[pids[j]]
            si = scores[ei.player_id]
            sj = scores[ej.player_id]

            # normalize scores
            ofs = min(0, si, sj)
            si -= ofs
            sj -= ofs
            if si + sj == 0:
                si, sj = 1, 1 # a draw

            # real score factor
            scorefactor_real = si / float(si + sj)

            # duels are done traditionally - a win nets
            # full points, not the score factor
            
            #if game.game_type_cd == 'duel':
            if False:
                # player i won
                if scorefactor_real > 0.5:
                    scorefactor_real = 1.0
                # player j won
                elif scorefactor_real < 0.5:
                    scorefactor_real = 0.0
                # nothing to do here for draws

            # expected score factor by elo
            elodiff = min(ep.maxlogdistance, max(-ep.maxlogdistance,
                (float(ei.elo) - float(ej.elo)) * ep.logdistancefactor))
            scorefactor_elo = 1 / (1 + math.exp(-elodiff))

            # initial adjustment values, which we may modify with additional rules
            adjustmenti = scorefactor_real - scorefactor_elo
            adjustmentj = scorefactor_elo - scorefactor_real
            
            if debug:
                print("Player i: {0}".format(ei.player_id))
                print("Player i's K: {0}".format(ei.k))
                print("Player j: {0}".format(ej.player_id))
                print("Player j's K: {0}".format(ej.k))
                print("Scorefactor real: {0}".format(scorefactor_real))
                print("Scorefactor elo: {0}".format(scorefactor_elo))
                print("adjustment i: {0}".format(adjustmenti))
                print("adjustment j: {0}".format(adjustmentj))

            if scorefactor_elo > 0.5:
            # player i is expected to win
                if scorefactor_real > 0.5:
                # he DID win, so he should never lose points.
                    adjustmenti = max(0, adjustmenti)
                else:
                # he lost, but let's make it continuous (making him lose less points in the result)
                    adjustmenti = (2 * scorefactor_real - 1) * scorefactor_elo
            else:
            # player j is expected to win
                if scorefactor_real > 0.5:
                # he lost, but let's make it continuous (making him lose less points in the result)
                    adjustmentj = (1 - 2 * scorefactor_real) * (1 - scorefactor_elo)
                else:
                # he DID win, so he should never lose points.
                    adjustmentj = max(0, adjustmentj)

            eloadjust[ei.player_id] += adjustmenti
            eloadjust[ej.player_id] += adjustmentj

    elo_deltas = {}
    for pid in pids:
        old_elo = float(elos[pid].elo)
        new_elo = max(float(elos[pid].elo) + eloadjust[pid] * elos[pid].k * ep.global_K / float(len(elos) - 1), ep.floor)
        elo_deltas[pid] = new_elo - old_elo
        
        debug_player = "kek"
        if debug_player in elo_deltas:
            print(debug_player + " elo delta: " + str(elo_deltas[debug_player]))

        elos[pid].elo = new_elo
        elos[pid].games += 1
        #elos[pid].update_dt = datetime.utcnow()

        #log.debug("Setting Player {0}'s Elo delta to {1}. Elo is now {2} (was {3}).".format(pid, elo_deltas[pid], new_elo, old_elo))
        if debug:
            print("Setting Player {0}'s Elo delta to {1}. Elo is now {2} (was {3}).".format(pid, elo_deltas[pid], new_elo, old_elo))

    return (elo_deltas, elos)

    

# parameters for K reduction
# this may be touched even if the DB already exists

KREDUCTION = KReduction(900, 75, 0.5, 5, 15, 0.2)

# parameters for chess elo
# only global_K may be touched even if the DB already exists
# we start at K=200, and fall to K=40 over the first 20 games
ELOPARMS = EloParms(global_K=200)


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


def prepare_player_elo_list(stats):
    """Make a list of guids to retrieve from ddb."""
    item_list = []
    for guid, player_stats in stats.items():
        item_list.append({"pk": "player", "sk": "playerinfo#" + guid})
    return item_list


def get_batch_items(item_list, ddb_table, log_stream_name):
    """Get items in a batch."""
    dynamodb = boto3.resource('dynamodb')
    item_info = "get_batch_items. Logstream: " + log_stream_name
    try:
        response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list, 'ProjectionExpression': 'sk, elos, real_name'}})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if len(response["Responses"][ddb_table.name]) > 0:
            result = response["Responses"][ddb_table.name]
        else:
            result = make_error_dict("[x] Item does not exist: ", item_info)
    return result


def update_player_info(ddb_table, elos, match_region_type, old_elos_ddb_maps):
    """Update existing players with new elo map."""
    # Example: ddb_table.update_item(Key=Key, UpdateExpression="set elos.#eloname = :elo, elos.#gamesname = :games", ExpressionAttributeNames={"#eloname": "na#6#elo", "#gamesname": "na#6#games"}, ExpressionAttributeValues={':elo': 134, ':games' : 135})
    update_expression="set elos = :elos"
    for guid, player_elo in elos.items():
        logger.info("Updating player: " + guid)
        key = { "pk": "player" , "sk": "playerinfo#" + guid }
        if guid in old_elos_ddb_maps:
            new_elos_map = old_elos_ddb_maps[guid]
        else:
            new_elos_map = {}
            
        new_elos_map[match_region_type + "#elo"] = int(round(player_elo.elo, 0))
        new_elos_map[match_region_type + "#games"] = player_elo.games
        expression_values = {':elos': new_elos_map}
        response = ddb_table.update_item(Key=key, 
                                         UpdateExpression=update_expression, 
                                         ExpressionAttributeValues=expression_values)

    
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
            start += 25


def ddb_prepare_eloprogress_items(elo_deltas, match_id):
    elo_delta_items = []
    for guid in elo_deltas:
        elo_item = {
            'pk'    : "eloprogress",
            'sk'    : "sk#" + guid + "#" + match_id,
            'data'  : int(round(elo_deltas[guid],0)),
            'lsipk' : "elomatch#" + match_id + "#" + guid
            }
        elo_delta_items.append(elo_item)
    return elo_delta_items