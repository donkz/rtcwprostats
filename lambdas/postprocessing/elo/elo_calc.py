"""
credit to https://github.com/PredatH0r/XonStat/blob/master/xonstat/elo.py .

ELO algorithm to calculate player ranks

"""
from datetime import datetime
import logging
import math
from botocore.exceptions import ClientError
import json
import boto3
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
    def __init__(self, global_K = 15, initial = 100, floor = 80, logdistancefactor = math.log(10)/float(400), maxlogdistance = math.log(10)):
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
        
        return k
    
# parameters for K reduction
# this may be touched even if the DB already exists

KREDUCTION = KReduction(900, 75, 0.5, 3, 10, 0.2)

# parameters for chess elo
# only global_K may be touched even if the DB already exists
# we start at K=200, and fall to K=40 over the first 20 games
ELOPARMS = EloParms(global_K=200)
  
def process_rtcwpro_elo(ddb_table, ddb_client, match_id, log_stream_name):
    "RTCWPro pipeline specific logic."
    t1 = _time.time()
    sk = match_id
    
    response = get_item("statsall", sk, ddb_table, log_stream_name)
    if "error" not in response:
        stats = json.loads(response["data"])
        logger.info("Retrieved statsall for " + str(len(stats)) + " players")
        stats = convert_stats_to_dict(stats)
    else:
        message = "Failed to retrieve statsall."
        logger.error(message)
        return message
        logger.error(json.dumps(response))
    
    response = get_item("wstatsall", sk, ddb_table, log_stream_name)
    if "error" not in response:
        wstats = json.loads(response["data"])
        logger.info("Retrieved wstatsall for " + str(len(wstats)) + " players")
    else:
        message = "Failed to retrieve wstatsall."
        logger.error(message)
        return message
        logger.error(json.dumps(response))

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
        message = "Failed to retrieve match."
        logger.error(message)
        return message
        logger.error(json.dumps(response))
        
    real_name_request_list = prepare_player_name_list(stats)
    response = get_batch_items(real_name_request_list, ddb_table, log_stream_name)
    real_names = {}
    if "error" not in response:
        for result in response:
            guid = result["pk"].split("#")[1]
            if "data" in result:
                real_names[guid] = result["data"]
                logger.info("Retrieved " + guid + " name: " + result["data"])

       
    elo_item_list = prepare_player_elo_list(stats, match_region_type)
    response = get_batch_items(elo_item_list, ddb_table, log_stream_name)

    elo_dict = {}
    elo_games = {}
    if "error" not in response:
        for result in response:
            guid = result["pk"].split("#")[1]
            if match_region_type in result["sk"]:
                elo_dict[guid] = result["data"]
                elo_games[guid] = int(result["games"]) 
                logger.info("Retrieved " + match_region_type + "#elo" + " " + " " + real_names.get(guid,"no_name").ljust(20) + " elo:" + str(elo_dict[guid]) + " games " + str(elo_games[guid]))

    else:
        logger.error("Failed to retrieve any player elos.")
        logger.error(json.dumps(response))
    
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
            logger.info(player_stats.get("alias", "missing_alias").ljust(20) + str(player_stats["categories"].get("kills", 0)).ljust(5) + str(win_multiplier).ljust(5) + str(score_step_3).ljust(5) + str(player_scores[guid].games).ljust(5))

    (elo_deltas, elos) = process_elos(player_scores, elo_dict)
    
    items = []
    elo_delta_items = ddb_prepare_eloprogress_items(player_scores, elos, elo_deltas, match_id, match_region_type, real_names)
    player_elo_items = ddb_prepare_player_elo_items(elos, match_region_type, real_names)
    items.extend(elo_delta_items)
    items.extend(player_elo_items)
    
    try:
        ddb_batch_write(ddb_client, ddb_table.name, items)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to load all eloprogress records for a match " + match_id + "\n" + error_msg
        logger.info(message)
        return message
    else:
        message = "Elo progress records inserted.\n"
    
    time_to_write = str(round((_time.time() - t1), 3))
    logger.info(f"Time to process ELOs is {time_to_write} s")
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


def prepare_player_elo_list(stats, match_region_type):
    """Make a list of guids to retrieve from ddb."""
    item_list = []
    for guid, player_stats in stats.items():
        item_list.append({"pk": "player#"+guid, "sk": "elo#" + match_region_type})
    return item_list

def prepare_player_name_list(stats):
    """Make a list of guids to retrieve from ddb."""
    item_list = []
    for guid, player_stats in stats.items():
        item_list.append({"pk": "player#"+guid, "sk": "realname"})
    return item_list


def get_batch_items(item_list, ddb_table, log_stream_name):
    """Get items in a batch."""
    dynamodb = boto3.resource('dynamodb')
    item_info = "get_batch_items. Logstream: " + log_stream_name
    try:
        response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list, 'ProjectionExpression': 'pk, sk, #data_value, real_name, games', 'ExpressionAttributeNames': {'#data_value': 'data'}}})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if len(response["Responses"][ddb_table.name]) > 0:
            result = response["Responses"][ddb_table.name]
        else:
            result = make_error_dict("[x] Item does not exist: ", item_info)
    return result
            
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


def ddb_prepare_eloprogress_items(player_scores, elos, elo_deltas, match_id, match_region_type, real_names):
    elo_delta_items = []
    
    for guid in elo_deltas:
        if guid in player_scores:
            performance_score = player_scores[guid].score
        if guid in elos:
            elo = elos[guid].elo
        elo_item = {
            'pk'    : "eloprogress#" + guid,
            'sk'    : match_region_type + "#" + match_id,
            'data'  : int(round(elo_deltas[guid],0)),
            'gsi1pk': "eloprogressmatch",
            'gsi1sk': match_id,
            'real_name': real_names.get(guid,""),
            'elo': int(round(elo, 0)),
            'performance_score': performance_score
            }
        elo_delta_items.append(elo_item)
    return elo_delta_items

def ddb_prepare_player_elo_items(elos, match_region_type, real_names):
    player_elo_items = []
    ts = datetime.now().isoformat()
    for guid, elo in elos.items():
        elo_item = ddb_prepare_elo_item(guid, match_region_type, elo.elo, elo.games, ts, real_names.get(guid,""))
        player_elo_items.append(elo_item)
    return player_elo_items

def ddb_prepare_elo_item(guid, match_region_type, elo, games, ts, real_name):  
    elo = int(round(elo, 0))
    elo_item = {
            'pk'            : "player"+ "#" + guid,
            'sk'            : "elo#" + match_region_type,
            # 'lsipk'         : "",
            'gsi1pk'        : "leaderelo#" + match_region_type,
            'gsi1sk'        : str(elo).zfill(3),
            'data'          : str(elo),
            'games'         : games,
            'updated'       : ts,
            "real_name"     : real_name
        }
    logger.info("Setting   " + guid + " " + real_name + " elo:" + str(elo) + " games " + str(games))
    return elo_item