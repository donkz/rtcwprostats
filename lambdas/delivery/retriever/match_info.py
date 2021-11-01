"""ANYTHING IN THIS FILE WILL BE SHARED WITH GROUP_CACHE.PY"""

import logging
log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("cacher_matchinfo_calc")
logger.setLevel(log_level)

def build_teams(new_total_stats):
    """ Bucket players into teams A and B."""
    
    debug = False
    game = 1
    teamA = []
    teamB = []
    assigned = False
    team_mapping = {}
    aliases = {}
    for match, match_stats in new_total_stats.items():
        current_axis = []
        current_allied = []
        
        # print("Processing match " + match)
        team_mapping[match] = {}
        team_mapping[match]["TeamB"] = "unset"
        team_mapping[match]["TeamA"] = "unset"
        for guid, player_stat in match_stats.items():
            # print("Processing player " + guid)
            # print("saw guy " + player_stat.get('alias',''))
            aliases[guid] = player_stat.get('alias','')
            
            if player_stat.get('team','Axis') == 'Axis': # TODO: rethink safety
                current_axis.append(guid)
            else:
                current_allied.append(guid)
        
            if game == 1:
                if player_stat.get('team','Axis') == 'Axis':
                    teamA.append(guid)
                    team_mapping[match]["TeamA"] = "Axis"
                else:
                    teamB.append(guid)
                    team_mapping[match]["TeamB"] = "Allied"
            else:
                if guid in teamA:
                    if debug: print(guid + " was already in a teamA")
                    if team_mapping[match]["TeamA"] == "unset":
                        team_mapping[match]["TeamA"]= player_stat.get('team','Axis')
                elif guid in teamB:
                    if team_mapping[match]["TeamB"] == "unset":
                        team_mapping[match]["TeamB"]= player_stat.get('team','Allied')
                    if debug: print(guid + " was already in a teamB")
                else:
                    """New joiner?"""
                    if debug: print(guid + " was not in a team")
                    assigned = False
                    if player_stat.get('team','Axis') == 'Axis':
                        if debug: print(guid + " appeared on Axis")
                        for axis_guid in current_axis:
                            if axis_guid in teamA:
                                if debug: print("another axis guid appeared on team A")
                                teamA.append(guid)
                                assigned = True
                                break
                    if player_stat.get('team','Axis') == 'Allied':
                        if debug: print(guid + " appeared on Allied")
                        for allied_guid in current_allied:
                            if allied_guid in teamA:
                                if debug: print("another allied guid appeared on team A")
                                teamA.append(guid)
                                assigned = True
                                break
                    if not assigned:
                        if debug: print("Assigning to team B")
                        teamB.append(guid)
        # print("Done with match " + match)
        game +=1
    
    alias_team_str = "TeamA:"
    for guid in teamA:
        alias_team_str += aliases[guid][0:12] + ","
    
    alias_team_str = alias_team_str[0:-1] + ";TeamB:"
    for guid in teamB:
        alias_team_str += aliases[guid][0:12] + ","
        
    return teamA, teamB, aliases, team_mapping, alias_team_str[0:-1]

def build_new_match_summary(match_dict, team_mapping):
    """ Take several matches in a gather and summarize them into one entity."""
    start = 0
    finish = 0
    group_info = {}
    results = {}
    finish_human = ""
    games = 0
    for match_round_id, matchinfo in match_dict.items():
                
        match_id = match_round_id[0:-1]
        
        if match_id not in results:
            results[match_id] = {}
            games += 1
        
        if start == 0:
            start = int(matchinfo.get("round_start",0))
        else:
            start = min(start,int(matchinfo.get("round_start",0)))
        finish = max(finish, int(matchinfo.get("round_end",0)))
        finish_human = max(finish_human, matchinfo.get("date_time_human",""))
        
        round_num = matchinfo.get("round",None)
        
        if round_num in ['1','2']:
            round_num_key = "round" + round_num
            results[match_id][round_num_key] = {}
            duration =  int(matchinfo.get("round_end",0)) - int(matchinfo.get("round_start",0))
            results[match_id][round_num_key]["duration"] = duration
            results[match_id][round_num_key]["duration_nice"] = seconds_to_minutes(duration)
            
            map_ = matchinfo.get("map","mp_fake")
            if "map" not in results[match_id]:
                results[match_id]["map"] = map_
            else:
                if results[match_id]["map"] != map_:
                    logger.warning("Different map in the same match " + map_)
        else:
            logger.warning("What round is it? " + matchinfo.get("round",'-1'))
        
    duration = finish - start
    duration_nice = seconds_to_minutes(duration)

    group_info["duration"] = duration
    group_info["duration_nice"] = duration_nice
    group_info["finish_human"] = finish_human
    group_info["games"] = games
    
    results = infer_winners_bandaid(results, match_dict)
    results = add_teamAB_maping(results, team_mapping)
    group_info["results"] = results
    return group_info

def infer_winners_bandaid(results, match_dict):
    """Determine winner of the game before this issue is closed and tested.
    https://github.com/rtcwmp-com/rtcwPro/issues/369 ."""
    
    offense_allied = ["mp_base", "mp_sub","braundorf_b7", "mp_password2", "mp_village","bd_bunker_b2", "mp_beach","te_adlernest_b1","te_cipher_b5","te_delivery_b1","te_escape2","te_frostbite"]
    offense_axis = ["mp_assault", "mp_ice", "te_kungfugrip"]
    
    for match_id, info in results.items():
        winner = "Draw"
        try:
            if "round2" in info:
                if "round1" not in info:
                    info["round1"] = {}
                    info["round1"]["duration"] = 600
                    logger.warning("winners_bandaid: Missing round1 info " + match_id + "2")
    
                if len(match_dict[match_id + "2"]["winner"].strip()) == 0:
                    logger.warning("winners_bandaid: Missing winner info for round2 " + match_id + "2" + " " + info["map"])
                    if info["round2"]["duration"] < info["round1"]["duration"]:
                        if info["map"] in offense_allied:
                            winner = "Allied"
                        elif info["map"] in offense_axis:
                            winner = "Axis"
                        else:
                            winner = "Allied" # most likely
                            logger.warning("winners_bandaid: Missing map condition to determine winner, setting default Allied")
                    elif info["round2"]["duration"] > info["round1"]["duration"]:
                        if info["map"] in offense_allied:
                            winner = "Axis"
                        elif info["map"] in offense_axis:
                            winner = "Allied"
                        else:
                            winner = "Axis" # most likely
                            logger.warning("winners_bandaid: Missing map condition to determine winner, setting default Allied")
                    elif info["round2"]["duration"] == info["round1"]["duration"]:
                        winner = "Draw"
                elif info["round2"]["duration"] == info["round1"]["duration"]:
                    winner = "Draw"
                else:
                    winner = match_dict[match_id + "2"]["winner"]
            else:
                # if round 2 is missing, people probably gave up in r1 and offence won?
                if info["map"] in offense_allied:
                    winner = "Allied"
                elif info["map"] in offense_axis:
                    winner = "Axis"
                else:
                    winner = "Allied" # most likely
                    logger.warning("winners_bandaid: Missing map condition to determine winner, setting default Allied")
        except:
            logger.warning("winners_bandaid: failed badly for " + match_id)
        finally:
            info["winner"] = winner
    
    return results

def add_teamAB_maping(results, team_mapping):
    """Add who is who (teamA was Axis in round 2) to final match results."""
    for match, result_set in results.items():
        if result_set["winner"] == team_mapping[match]["TeamA"]:
            winnerAB = "TeamA"
        elif result_set["winner"] == team_mapping[match]["TeamB"]:
            winnerAB = "TeamB"
        else:
            logger.warning("add_teamAB_maping could not determine winnerAB for match:" + match)
            winnerAB = "TeamB"
        result_set["winnerAB"] = winnerAB
    return results
    

def seconds_to_minutes(duration):
    """Convert int seconds to string minutes."""
    duration_nice = str(int(duration/60)).zfill(2) + ":" + str(duration%60).zfill(2)
    return duration_nice

def convert_stats_to_dict(stats):
    """Convert stats list to dict for easier processing."""
    
    if len(stats) == 2 and len(stats[0]) > 1: #stats grouped in teams in a list of 2 teams , each team over 1 player
        stats_tmp = stats[0].copy()
        stats_tmp.update(stats[1])
    else:
        stats_tmp = {}
        for player in stats:
            stats_tmp.update(player)
    return stats_tmp