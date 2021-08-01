import json
import pandas as pd
import os
import shutil

directory = r".\gamestats3\\"
directory = r"C:\c\.wolf-mine\rtcwpro\stats"
directory = r"C:\c\adlad-ams\rtcwpro\stats"
directory = r"C:\c\adlad-london\rtcwpro\stats"
directory = r"C:\c\murkey\rtcwpro\stats"
directory = r"C:\c\virkes\rtcwpro\stats"
directory_rejects_twofers = r"C:\Temp\.wolf-rejects\twofers"
directory_rejects_blankmatchid = r"C:\Temp\.wolf-rejects\blank_match"
directory_rejects_notjson = r"C:\Temp\.wolf-rejects\notjson"
directory_rejects_restarts = r"C:\Temp\.wolf-rejects\restarts"

jsons = []

for subdir, dirs, files in os.walk(directory):
    for file in files:
        # print(os.path.join(subdir, file))
        filepath = os.path.join(subdir, file)

        with open(filepath) as f:
            try:
                value = json.load(f)
            except Exception as ex:
                print(ex)
                f.close()
                print("Failed to load " + filepath + " Moving to " + directory_rejects_notjson)
                shutil.move(filepath, os.path.join(directory_rejects_notjson, file))
                continue
        if "gameinfo" not in value:
            if "map_restart" in json.dumps(value):
                print("No gameinfo. Map was restarted. Moving to " + directory_rejects_restarts)
                shutil.move(filepath, os.path.join(directory_rejects_restarts, file))
        elif len(value["stats"]) == 2:
            print("Match has 2 list items under stats. Moving to " + directory_rejects_twofers)
            shutil.move(filepath, os.path.join(directory_rejects_twofers, file))
        elif value["gameinfo"].get('match_id', 'na') in ["", "na"]:
            print("Match has no match_id. Moving to " + directory_rejects_blankmatchid)
            shutil.move(filepath, os.path.join(directory_rejects_blankmatchid, file))
        else:
            jsons.append(value)

jsoncols_root_cols = ['serverinfo', 'gameinfo', 'gamelog', 'stats', 'wstats']


def dict_differences(cols, json, section):
    """Find differences between expected list of columns and current json."""
    ab = [x for x in list(json.keys()) if x not in cols]
    if len(ab) > 0:
        if str(ab) != "['categories']":
            print("[x] new columns in json " + section + " " + str(ab))


servercols = []
servercols.append('gameVersion')
servercols.append('g_gameStatslog')
servercols.append('g_gametype')
servercols.append('jsonGameStatVersion')
servercols.append('serverIP')
servercols.append('serverName')
servercols.append('unixtime')  # game id
servercols.append('g_customConfig')


gamecols = []
gamecols.append('match_id')  # pk
gamecols.append('round')  # pk
gamecols.append('round_start')
gamecols.append('round_end')
gamecols.append('map')
gamecols.append('time_limit')
gamecols.append('allies_cycle')
gamecols.append('axis_cycle')
gamecols.append('winner')


logcols = []
logcols.append('match_id')  # fk
logcols.append('round')  # fk
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
statcols.append('match_id')  # fk
statcols.append('round')  # fk
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
weaponcols.append('match_id')  # fk
weaponcols.append('round')  # fk
weaponcols.append('GUID')  # fk
# weaponcols.append('alias')  # fk
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
    if "gameinfo" not in j:
        if "map_restart" in json.dumps(j):
            print("No gameinfo. Map was restarted")
        else:
            print("No gameinfo in " + str(j))

    elif "stats" not in j:
        print("No stats in " + j["gameinfo"].get('match_id', 'na'))
    elif isinstance(j["stats"], list):
        if len(j["stats"]) == 2:
            print(j["gameinfo"].get('match_id', 'na') + " is a list of " + str(len(j["stats"])))

        if j["gameinfo"].get('match_id', 'na') in ["", "na"]:
            print("match_id is empty\n")
            print(j["gameinfo"])
    elif isinstance(j["stats"], dict):
        print(j["gameinfo"].get('match_id', 'na') + " is a dict of " + str(len(j["stats"].keys())))
    else:
        print(j["gameinfo"].get('match_id', 'na') + " is a " + type(j["stats"]))
    dict_differences(jsoncols_root_cols, j, "root")

    dict_differences(servercols, j["serverinfo"], "serverinfo")
    server = []
    server.append(j["serverinfo"].get('gameVersion', 'na'))
    server.append(j["serverinfo"].get('g_gameStatslog', 'na'))
    server.append(j["serverinfo"].get('g_gametype', 'na'))
    server.append(j["serverinfo"].get('jsonGameStatVersion', 'na'))
    server.append(j["serverinfo"].get('serverIP', 'na'))
    server.append(j["serverinfo"].get('serverName', 'na'))
    server.append(j["serverinfo"].get('unixtime', 'na'))
    server.append(j["serverinfo"].get('g_customConfig', 'na'))
    servers.append(server)

    if "gameinfo" in j:
        dict_differences(gamecols, j["gameinfo"], "gameinfo")
        game = []
        game.append(j["gameinfo"].get('match_id', 'na'))  # pk
        game.append(j["gameinfo"].get('round', 'na'))  # pk
        game.append(j["gameinfo"].get('round_start', 'na'))
        game.append(j["gameinfo"].get('round_end', 'na'))
        game.append(j["gameinfo"].get('map', 'na'))
        game.append(j["gameinfo"].get('time_limit', 'na'))
        game.append(j["gameinfo"].get('allies_cycle', 'na'))
        game.append(j["gameinfo"].get('axis_cycle', 'na'))
        game.append(j["gameinfo"].get('winner', 'na'))
        games.append(game)
    else:
        continue

    for line in j["gamelog"]:
        dict_differences(logcols, line, "gamelog")
        log = []

        log.append(j["gameinfo"].get('match_id'))  # fk
        log.append(j["gameinfo"].get('round'))  # fk
        log.append(line.get('unixtime', 'na'))
        log.append(line.get('group', 'na'))
        log.append(line.get('label', 'na'))
        log.append(line.get('agent', 'na'))
        log.append(line.get('other', 'na'))
        log.append(line.get('weapon', 'na'))
        log.append(line.get('killer_health', 'na'))
        log.append(line.get('Axis', 'na'))
        log.append(line.get('Allied', 'na'))
        log.append(line.get('other_health', 'na'))
        log.append(line.get('agent_pos', 'na'))
        log.append(line.get('agent_angle', 'na'))
        log.append(line.get('other_pos', 'na'))
        log.append(line.get('other_angle', 'na'))
        log.append(line.get('allies_alive', 'na'))
        log.append(line.get('axis_alive', 'na'))
        logs.append(log)

    stats_new_object = []
    if len(j["stats"]) == 2:
        for k, v in j["stats"][0].items():
            stats_new_object.append({k: v})
        for k, v in j["stats"][1].items():
            stats_new_object.append({k: v})
    else:
        stats_new_object = j["stats"]

    # for team, teaminfo  in j["stats"].items():
    for player_item in stats_new_object:
        stat = []
        stat.append(j["gameinfo"].get('match_id'))  # fk
        stat.append(j["gameinfo"].get('round'))  # fk

        for playerguid, playerstat in player_item.items():
            dict_differences(statcols, playerstat, "playerstat")
            stat.append(playerstat.get('team', 'na'))
            stat.append(playerguid)
            stat.append(playerstat.get('alias', 'na'))
            stat.append(playerstat.get('start_time', 'na'))
            stat.append(playerstat.get('num_rounds', 'na'))
            if 'categories' in playerstat:
                stat_values_dict = playerstat['categories']
            else:
                stat_values_dict = playerstat
            stat.append(stat_values_dict.get('kills', 'na'))
            stat.append(stat_values_dict.get('deaths', 'na'))
            stat.append(stat_values_dict.get('gibs', 'na'))
            stat.append(stat_values_dict.get('suicides', 'na'))
            stat.append(stat_values_dict.get('teamkills', 'na'))
            stat.append(stat_values_dict.get('headshots', 'na'))
            stat.append(stat_values_dict.get('damagegiven', 'na'))
            stat.append(stat_values_dict.get('damagereceived', 'na'))
            stat.append(stat_values_dict.get('damageteam', 'na'))
            stat.append(stat_values_dict.get('hits', 'na'))
            stat.append(stat_values_dict.get('shots', 'na'))
            stat.append(stat_values_dict.get('accuracy', 'na'))
            stat.append(stat_values_dict.get('revives', 'na'))
            stat.append(stat_values_dict.get('ammogiven', 'na'))
            stat.append(stat_values_dict.get('healthgiven', 'na'))
            stat.append(stat_values_dict.get('poisoned', 'na'))
            stat.append(stat_values_dict.get('knifekills', 'na'))
            stat.append(stat_values_dict.get('killpeak', 'na'))
            stat.append(stat_values_dict.get('efficiency', 'na'))
            stat.append(stat_values_dict.get('score', 'na'))
            stat.append(stat_values_dict.get('dyn_planted', 'na'))
            stat.append(stat_values_dict.get('dyn_defused', 'na'))
            stat.append(stat_values_dict.get('obj_captured', 'na'))
            stat.append(stat_values_dict.get('obj_destroyed', 'na'))
            stat.append(stat_values_dict.get('obj_returned', 'na'))
            stat.append(stat_values_dict.get('obj_taken', 'na'))
            stats.append(stat)

    for player_item in j["wstats"]:
        for playerguid, weapon_items in player_item.items():
            for weapon in weapon_items:
                # print(playerguid, weapon)
                wstat = []
                dict_differences(weaponcols, weapon, "weapon")
                wstat.append(j["gameinfo"].get('match_id'))
                wstat.append(j["gameinfo"].get('round'))
                wstat.append(playerguid)
                # wstat.append(playerstat.get('alias', 'na'))
                wstat.append(weapon.get('weapon', 'na'))
                wstat.append(weapon.get('kills', 'na'))
                wstat.append(weapon.get('deaths', 'na'))
                wstat.append(weapon.get('headshots', 'na'))
                wstat.append(weapon.get('hits', 'na'))
                wstat.append(weapon.get('shots', 'na'))
                weapons.append(wstat)

serversdf = pd.DataFrame(servers, columns=servercols)
gamesdf = pd.DataFrame(games, columns=gamecols)
logsdf = pd.DataFrame(logs, columns=logcols)
statsdf = pd.DataFrame(stats, columns=statcols)
weapondf = pd.DataFrame(weapons, columns=weaponcols)

serversdf.to_csv("serversdf.csv")
gamesdf.to_csv("games.csv")
logsdf.to_csv("logs.csv")
statsdf.to_csv("stats.csv")
weapondf.to_csv("weapon.csv")

pd.options.display.max_rows = 999
pd.options.display.max_columns = 20
pd.options.display.width = 300
pd.set_option("expand_frame_repr", True)
pd.set_option("max_colwidth", 20)
pd.set_option("precision", 2)

print("Servers--------")
print(serversdf.describe())
print("Matches--------")
print(gamesdf.describe())
print("Logs--------")
print(logsdf.describe())
print("Stats1--------")
print(statsdf.iloc[:,0:20].describe())
print("Stats1--------")
print(statsdf.iloc[:,20:40].describe())
print("Weapons--------")
print(weapondf.describe())



def json_extract(obj, skeleton):
    """Recursively fetch values from nested JSON."""
    if skeleton is None:
        skeleton = {}

    def extract(obj, skeleton):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    print("Extracting " + k)
                    extract(v, skeleton)
                skeleton[k] = skeleton.get(k, 0) + 1
        elif isinstance(obj, list):
            for item in obj:
                extract(item, skeleton)
        return skeleton

    values = extract(obj, skeleton)
    return values
