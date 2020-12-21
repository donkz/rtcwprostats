import json
import os
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import mapper

from installdb.dbconnection import get_db_connection_string

#! denotes difference between class property and json field name
class MatchLine(object):
    
    def __init__(self):
        self.unixtime = 1
        self.serverName = "Default server name"
        self.serverIP = "256.256.256.256"
        self.gameVersion = "0.0"
        self.jsonGameStatVersion = "0.0"
        self.g_gametype = 6
        self.date = "1970-01-01T00:00:01Z"
        
        self.map_name = "mp_nothing"
        self.levelTime = "0:00"
        self.round_num = 1
        
    def __repr__(self):
        return "<Match(unixtime='%s', date='%s', map_name='%s' , round_num='%s')>" % (self.unixtime, self.date, self.map_name, self.round_num) 
    
class LogLine(object):
    
    def __init__(self):
        self.round_id = 1
        self.event_order = 1
        self.event = 'kill'
        self.unixtime = 1
        self.levelTime = "0:00"
        self.team = 'Axis'
        self.player = 'na'
        self.killer = 'na'
        self.victim = 'na'
        self.weapon = 'na'
        self.khealth = 100
        self.result = 'na'
        
    def __repr__(self):
        return "<LogLine(gameunixtime='%s', event_order='%s', event='%s' , round_num='%s')>" % (self.gameunixtime, self.event_order, self.event, self.weapon) 

class StatLine(object):
    
    def __init__(self):
        self.round_id = 1
        self.GUID = "na"
        self.alias = "na"
        self.team = "Axis"
        self.start_time = 1
        self.end_time = 2
        self.rounds = 1
        self.kills = 0
        self.deaths = 0
        self.gibs = 0
        self.suicides = 0
        self.teamkills = 0
        self.headshots = 0
        self.damagegiven = 0
        self.damagereceived = 0
        self.damageteam = 0
        self.hits = 0
        self.shots = 0
        self.accuracy = 0
        self.revives = 0
        self.ammogiven = 0
        self.healthgiven = 0
        self.poisoned = 0
        self.knifekills = 0
        self.killpeak = 0
        self.efficiency = 0
        self.score = 0
        self.dyn_planted = 0
        self.dyn_defused = 0
        self.obj_captured = 0
        self.obj_destroyed = 0
        self.obj_returned = 0
        self.obj_taken = 0
        
class WeaponLine(object):
    
    def __init__(self):
        self.round_id = 1
        self.GUID = "na"
        self.alias = "na"
        self.weapon = "na"
        self.kills = 0 
        self.deaths = 0 
        self.headshots = 0
        self.hits = 0 
        self.shots = 0

    
#    player = Player()
#    session.add(player)
#    session.flush()
        
directory = r"..\..\..\\test\gamestats\\"

engine = sqlalchemy.create_engine(get_db_connection_string(environment = "prod"), echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

#DBSession = scoped_session(sessionmaker(autocommit=True))
#DBSession.configure(bind=engine)
Base.metadata.bind = engine
#Base.metadata.create_all(engine)
MetaData = sqlalchemy.MetaData(bind=engine)
MetaData.reflect()

matches_table = MetaData.tables['matches']
mapper(MatchLine, matches_table)

logs_table = MetaData.tables['logs']
mapper(LogLine, logs_table)

stats_table = MetaData.tables['stats']
mapper(StatLine, stats_table)

weapons_table = MetaData.tables['weapons']
mapper(WeaponLine, weapons_table)

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

for j in jsons:  
    match = MatchLine()
    for (key, value) in j["gameinfo"].items():
        if key == 'serverName': match.serverName = value
        if key == 'serverIP': match.serverIP = value
        if key == 'gameVersion': match.gameVersion = value
        if key == 'gameinfo': match.gameinfo = value
        if key == 'g_gametype': match.g_gametype = value
        if key == 'date': match.date = value
        if key == 'unixtime': match.unixtime = value
        if key == 'map': match.map_name = value
        if key == 'levelTime': match.levelTime = value
        if key == 'round': match.round_num = value
    session.add(match)
    #session.flush()
    
    
    for line in j["gamelog"]:
        logline = LogLine()
        logline.round_id = match.unixtime
        for (key, value) in line.items():
            if key == 'event_order': logline.event_order = value
            if key == 'event': logline.event = value
            if key == 'unixtime': logline.unixtime = value
            if key == 'levelTime': logline.levelTime = value
            if key == 'team': logline.team = value
            if key == 'player': logline.player = value
            if key == 'killer': logline.killer = value
            if key == 'victim': logline.victim = value
            if key == 'weapon': logline.weapon = value
            if key == 'khealth': logline.khealth = value
            if key == 'result': logline.result = value
        session.add(logline)

    
    
    for playerstat in j["players"]:
        stat = StatLine()
        stat.round_id = match.unixtime
        for (key, value) in line.items():
            if key == 'GUID': stat.GUID = value
            if key == 'alias': stat.alias = value
            if key == 'team': stat.team = value
            if key == 'start_time': stat.start_time = value
            if key == 'end_time': stat.end_time = value
            if key == 'rounds': stat.rounds = value
            if key == 'kills': stat.kills = value
            if key == 'deaths': stat.deaths = value
            if key == 'gibs': stat.gibs = value
            if key == 'suicides': stat.suicides = value
            if key == 'teamkills': stat.teamkills = value
            if key == 'headshots': stat.headshots = value
            if key == 'damagegiven': stat.damagegiven = value
            if key == 'damagereceived': stat.damagereceived = value
            if key == 'damageteam': stat.damageteam = value
            if key == 'hits': stat.hits = value
            if key == 'shots': stat.shots = value
            if key == 'accuracy': stat.accuracy = value
            if key == 'revives': stat.revives = value
            if key == 'ammogiven': stat.ammogiven = value
            if key == 'healthgiven': stat.healthgiven = value
            if key == 'poisoned': stat.poisoned = value
            if key == 'knifekills': stat.knifekills = value
            if key == 'killpeak': stat.killpeak = value
            if key == 'efficiency': stat.efficiency = value
            if key == 'score': stat.score = value
            if key == 'dyn_planted': stat.dyn_planted = value
            if key == 'dyn_defused': stat.dyn_defused = value
            if key == 'obj_captured': stat.obj_captured = value
            if key == 'obj_destroyed': stat.obj_destroyed = value
            if key == 'obj_returned': stat.obj_returned = value
            if key == 'obj_taken': stat.obj_taken = value
        session.add(stat)
        
        
        for weapon in playerstat["wstats"]:
            wstat = WeaponLine()
            wstat.round_id = match.unixtime
            wstat.GUID = stat.GUID
            wstat.alias = stat.alias
            for (key, value) in line.items():
                if key == 'weapon': wstat.weapon = value
                if key == 'kills': wstat.kills = value 
                if key == 'deaths': wstat.deaths = value 
                if key == 'headshots': wstat.headshots = value
                if key == 'hits': wstat.hits = value 
                if key == 'shots': wstat.shots = value
            session.add(wstat)
            
session.commit()


