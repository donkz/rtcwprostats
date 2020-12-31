import json
import pandas as pd
import os

directory = r".\gamestats3\\"

jsons = []
for filename in os.listdir(directory):
    if filename.endswith(".json"): 
         print("Processing " + filename)
         with open(directory + filename) as f:
            try:
                value = json.load(f)
                jsons.append(value)
            except:
                print("Failed to load " + filename)

jsoncols_root_cols = ['serverinfo', 'gameinfo', 'gamelog', 'stats', 'wstats']
def dict_differences(cols, json, section):
    ab = [x for x in list(json.keys()) if x not in cols]
#    ba = [x for x in cols if x not in list(json.keys())]
    if len(ab) > 0:
        print("[x] new columns in json " + section + " " + str(ab))
#    if len(ba) > 0:
#        print("[x] columns missing in json" + str(ab))

servercols = []
servercols.append('gameVersion')
servercols.append('g_gameStatslog')
servercols.append('g_gametype')
servercols.append('jsonGameStatVersion')
servercols.append('serverIP')
servercols.append('serverName')
servercols.append('unixtime') #game id
servercols.append('g_customConfig')


gamecols = []
gamecols.append('match_id') #pk
gamecols.append('round') #pk
gamecols.append('round_start')
gamecols.append('round_end')
gamecols.append('map')
gamecols.append('time_limit')
gamecols.append('allies_cycle')
gamecols.append('axis_cycle')
gamecols.append('winner')


logcols = []
logcols.append('match_id') #fk
logcols.append('round') #fk
logcols.append('unixtime')
logcols.append('group')
logcols.append('label')
logcols.append('agent')
logcols.append('other')
logcols.append('weapon')
logcols.append('killer_health')
logcols.append('Axis')
logcols.append('Allied')
logcols.append('other_health')
logcols.append('agent_pos')
logcols.append('agent_angle')
logcols.append('other_pos')
logcols.append('other_angle')
logcols.append('allies_alive')
logcols.append('axis_alive')


statcols = []
statcols.append('match_id') #fk
statcols.append('round') #fk
statcols.append('team')
statcols.append('GUID')
statcols.append('alias')
statcols.append('start_time')
statcols.append('num_rounds')
statcols.append('kills')
statcols.append('deaths')
statcols.append('gibs')
statcols.append('suicides')
statcols.append('teamkills')
statcols.append('headshots')
statcols.append('damagegiven')
statcols.append('damagereceived')
statcols.append('damageteam')
statcols.append('hits')
statcols.append('shots')
statcols.append('accuracy')
statcols.append('revives')
statcols.append('ammogiven')
statcols.append('healthgiven')
statcols.append('poisoned')
statcols.append('knifekills')
statcols.append('killpeak')
statcols.append('efficiency')
statcols.append('score')
statcols.append('dyn_planted')
statcols.append('dyn_defused')
statcols.append('obj_captured')
statcols.append('obj_destroyed')
statcols.append('obj_returned')
statcols.append('obj_taken')

weaponcols = []
weaponcols.append('match_id') #fk
weaponcols.append('round') #fk
weaponcols.append('GUID') #fk
weaponcols.append('alias') #fk
weaponcols.append('weapon')
weaponcols.append('kills') 
weaponcols.append('deaths') 
weaponcols.append('headshots')
weaponcols.append('hits') 
weaponcols.append('shots')


servers = []
games = []
stats = []
weapons = []
logs = []
for j in jsons:
    dict_differences(jsoncols_root_cols, j, "root")
    
    dict_differences(servercols, j["serverinfo"], "serverinfo")
    server = []    
    server.append(j["serverinfo"].get('gameVersion','na'))
    server.append(j["serverinfo"].get('g_gameStatslog','na'))
    server.append(j["serverinfo"].get('g_gametype','na'))
    server.append(j["serverinfo"].get('jsonGameStatVersion','na'))
    server.append(j["serverinfo"].get('serverIP','na'))
    server.append(j["serverinfo"].get('serverName','na'))
    server.append(j["serverinfo"].get('unixtime','na'))
    servers.append(server)
    
    dict_differences(gamecols, j["gameinfo"], "gameinfo")
    game = []
    game.append(j["gameinfo"].get('match_id','na')) #pk
    game.append(j["gameinfo"].get('round','na')) #pk
    game.append(j["gameinfo"].get('round_start','na'))
    game.append(j["gameinfo"].get('round_end','na'))
    game.append(j["gameinfo"].get('map','na'))
    game.append(j["gameinfo"].get('time_limit','na'))
    game.append(j["gameinfo"].get('allies_cycle','na'))
    game.append(j["gameinfo"].get('axis_cycle','na'))
    game.append(j["gameinfo"].get('winner','na'))
    games.append(game)

    
    for line in j["gamelog"]:
        dict_differences(logcols, line, "gamelog")
        log = []
        
        log.append(j["gameinfo"].get('match_id')) #fk
        log.append(j["gameinfo"].get('round')) #fk
        log.append(line.get('unixtime','na'))
        log.append(line.get('group','na'))
        log.append(line.get('label','na'))
        log.append(line.get('agent','na'))
        log.append(line.get('other','na'))
        log.append(line.get('weapon','na'))
        log.append(line.get('killer_health','na'))
        log.append(line.get('Axis','na'))
        log.append(line.get('Allied','na'))
        logs.append(log)
        
        
        
    for team, teaminfo  in j["stats"].items():
        stat = []
        
        stat.append(j["gameinfo"].get('match_id')) #fk
        stat.append(j["gameinfo"].get('round')) #fk
        
        for playerguid, playerstat in teaminfo.items():
            dict_differences(statcols, playerstat, "playerstat")
            stat.append(team)
            stat.append(playerguid)
            stat.append(playerstat.get('alias','na'))
            stat.append(playerstat.get('start_time','na'))
            stat.append(playerstat.get('num_rounds','na'))
            stat.append(playerstat.get('kills','na'))
            stat.append(playerstat.get('deaths','na'))
            stat.append(playerstat.get('gibs','na'))
            stat.append(playerstat.get('suicides','na'))
            stat.append(playerstat.get('teamkills','na'))
            stat.append(playerstat.get('headshots','na'))
            stat.append(playerstat.get('damagegiven','na'))
            stat.append(playerstat.get('damagereceived','na'))
            stat.append(playerstat.get('damageteam','na'))
            stat.append(playerstat.get('hits','na'))
            stat.append(playerstat.get('shots','na'))
            stat.append(playerstat.get('accuracy','na'))
            stat.append(playerstat.get('revives','na'))
            stat.append(playerstat.get('ammogiven','na'))
            stat.append(playerstat.get('healthgiven','na'))
            stat.append(playerstat.get('poisoned','na'))
            stat.append(playerstat.get('knifekills','na'))
            stat.append(playerstat.get('killpeak','na'))
            stat.append(playerstat.get('efficiency','na'))
            stat.append(playerstat.get('score','na'))
            stat.append(playerstat.get('dyn_planted','na'))
            stat.append(playerstat.get('dyn_defused','na'))
            stat.append(playerstat.get('obj_captured','na'))
            stat.append(playerstat.get('obj_destroyed','na'))
            stat.append(playerstat.get('obj_returned','na'))
            stat.append(playerstat.get('obj_taken','na'))
            stats.append(stat)
        
        for weapon in playerstat["wstats"]:
            wstat = []
            dict_differences(weaponcols, weapon, "weapon")
            wstat.append(j["gameinfo"].get('match_id'))
            wstat.append(j["gameinfo"].get('round'))
            wstat.append(playerguid)
            wstat.append(playerstat.get('alias','na'))
            wstat.append(weapon.get('weapon','na'))
            wstat.append(weapon.get('kills','na')) 
            wstat.append(weapon.get('deaths','na')) 
            wstat.append(weapon.get('headshots','na'))
            wstat.append(weapon.get('hits','na')) 
            wstat.append(weapon.get('shots','na'))
            weapons.append(wstat)

serversdf =  pd.DataFrame(servers, columns=servercols)             
gamesdf =  pd.DataFrame(games, columns=gamecols) 
logsdf = pd.DataFrame(logs, columns=logcols)
statsdf = pd.DataFrame(stats, columns=statcols)    
weapondf = pd.DataFrame(weapons, columns=weaponcols) 


def json_extract(obj, skeleton):
    """Recursively fetch values from nested JSON."""
    if skeleton == None:
        skeleton = {}

    def extract(obj, skeleton):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    print("Extracting " + k)
                    extract(v, skeleton)
                skeleton[k]=skeleton.get(k,0) +1 
        elif isinstance(obj, list):
            for item in obj:
                extract(item, skeleton)
        return skeleton

    values = extract(obj, skeleton)
    return values
