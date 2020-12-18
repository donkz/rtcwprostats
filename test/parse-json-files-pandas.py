import json
import pandas as pd
import os

directory = r".\gamestats\\"

jsons = []
for filename in os.listdir(directory):
    if filename.endswith(".log"): 
         print("Processing " + filename)
         with open(directory + filename) as f:
            #jsons.append(json.load(f))
            try:
                value = json.load(f)
                jsons.append(value)
            except:
                print("Failed to load " + filename)

matchcols = []
matchcols.append('serverName')
matchcols.append('serverIP')
matchcols.append('gameVersion')
matchcols.append('jsonGameStatVersion')
matchcols.append('g_gametype')
matchcols.append('date')
matchcols.append('unixtime')
matchcols.append('map')
matchcols.append('levelTime')
matchcols.append('round')

logcols = []
logcols.append('round_id') #fk
logcols.append('event_order')
logcols.append('event')
logcols.append('unixtime')
logcols.append('levelTime')
logcols.append('team')
logcols.append('player')
logcols.append('killer')
logcols.append('victim')
logcols.append('weapon')
logcols.append('khealth')
logcols.append('result')

statcols = []
statcols.append('round_id') #fk
statcols.append('GUID')
statcols.append('alias')
statcols.append('team')
statcols.append('start_time')
statcols.append('end_time')
statcols.append('rounds')
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
# 'wstats'

weaponcols = []
weaponcols.append('round_id') #fk
weaponcols.append('GUID') #fk
weaponcols.append('alias') #fk optional
weaponcols.append('weapon')
weaponcols.append('kills') 
weaponcols.append('deaths') 
weaponcols.append('headshots')
weaponcols.append('hits') 
weaponcols.append('shots')


matches = []
stats = []
weapons = []
logs = []
for j in jsons:  
    match = []  
    match.append(j["gameinfo"].get('serverName','na'))
    match.append(j["gameinfo"].get('serverIP','na'))
    match.append(j["gameinfo"].get('gameVersion','na'))
    match.append(j["gameinfo"].get('jsonGameStatVersion','na'))
    match.append(j["gameinfo"].get('g_gametype','na'))
    match.append(j["gameinfo"].get('date','na'))
    match.append(j["gameinfo"].get('unixtime','na'))
    match.append(j["gameinfo"].get('map','na'))
    match.append(j["gameinfo"].get('levelTime','na'))
    match.append(j["gameinfo"].get('round','na'))
    matches.append(match)
    
    for line in j["gamelog"]:
        log = []
        log.append(j["gameinfo"].get('unixtime','na'))
        log.append(line.get('event_order','na'))
        log.append(line.get('event','na'))
        log.append(line.get('unixtime','na'))
        log.append(line.get('levelTime','na'))
        log.append(line.get('team','na'))
        log.append(line.get('player','na'))
        log.append(line.get('killer','na'))
        log.append(line.get('victim','na'))
        log.append(line.get('weapon','na'))
        log.append(line.get('khealth','na'))
        log.append(line.get('result','na'))
        logs.append(log)

    
    for playerstat in j["players"]:
        stat = []
        stat.append(j["gameinfo"].get('unixtime','na'))
        stat.append(playerstat.get('GUID','na'))
        stat.append(playerstat.get('alias','na'))
        stat.append(playerstat.get('team','na'))
        stat.append(playerstat.get('start_time','na'))
        stat.append(playerstat.get('end_time','na'))
        stat.append(playerstat.get('rounds','na'))
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
            wstat.append(j["gameinfo"].get('unixtime','na'))
            wstat.append(playerstat.get('GUID','na'))
            wstat.append(playerstat.get('alias','na'))
            wstat.append(weapon.get('weapon','na'))
            wstat.append(weapon.get('kills','na')) 
            wstat.append(weapon.get('deaths','na')) 
            wstat.append(weapon.get('headshots','na'))
            wstat.append(weapon.get('hits','na')) 
            wstat.append(weapon.get('shots','na'))
            weapons.append(wstat)

matchesdf =  pd.DataFrame(matches, columns=matchcols)             
logsdf = pd.DataFrame(logs, columns=logcols)
statsdf = pd.DataFrame(stats, columns=statcols)    
weapondf = pd.DataFrame(weapons, columns=weaponcols) 



