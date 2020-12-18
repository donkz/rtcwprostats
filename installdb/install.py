#import os
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String   
from sqlalchemy.orm import sessionmaker

engine = sqlalchemy.create_engine('sqlite:///..//test//database.db', echo=True)
Base = declarative_base()
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)
session = Session()

#! denotes difference between class property and json field name
class MatchTable(Base):
    __tablename__ = 'matches'
    
    unixtime = Column(Integer, primary_key=True)
    serverName = Column(String(200))
    serverIP = Column(String(15))
    gameVersion = Column(String(20))
    jsonGameStatVersion = Column(String(20))
    g_gametype = Column(Integer)
    date = Column(String(30))
    
    map_name = Column(String(30)) #!
    levelTime = Column(String(5))
    round_num = Column(Integer) #!
    
    def __repr__(self):
        return "<Match(unixtime='%s', date='%s', map_name='%s' , round_num='%s')>" % (self.unixtime, self.date, self.map_name, self.round_num)

class LogTable(Base):
    __tablename__ = 'logs'
    
    round_id = Column(Integer, primary_key=True)
    event_order = Column(Integer, primary_key=True)
    event_order = Column(String(20))
    unixtime = Column(Integer)
    levelTime = Column(String(5))
    team = Column(String(6))
    player = Column(String(30))
    killer = Column(String(30))
    victim = Column(String(30))
    weapon = Column(String(30))
    khealth = Column(Integer)
    result = Column(String(100))
    
    def __repr__(self):
        return "<Log(roundid='%s', event_order='%s', event_order='%s' , weapon='%s')>" % (self.round_id, self.event_order, self.event_order, self.weapon)

class StatsTable(Base):
    __tablename__ = 'stats'
    round_id = Column(Integer, primary_key=True)
    GUID = Column(String(30), primary_key=True)
    alias = Column(String(30))
    team = Column(String(6))
    start_time = Column(Integer)
    end_time = Column(Integer)
    rounds = Column(Integer)
    kills = Column(Integer)
    deaths = Column(Integer)
    gibs = Column(Integer)
    suicides = Column(Integer)
    teamkills = Column(Integer)
    headshots = Column(Integer)
    damagegiven = Column(Integer)
    damagereceived = Column(Integer)
    damageteam = Column(Integer)
    hits = Column(Integer)
    shots = Column(Integer)
    accuracy = Column(Integer)
    revives = Column(Integer)
    ammogiven = Column(Integer)
    healthgiven = Column(Integer)
    poisoned = Column(Integer)
    knifekills = Column(Integer)
    killpeak = Column(Integer)
    efficiency = Column(Integer)
    score = Column(Integer)
    dyn_planted = Column(Integer)
    dyn_defused = Column(Integer)
    obj_captured = Column(Integer)
    obj_destroyed = Column(Integer)
    obj_returned = Column(Integer)
    obj_taken = Column(Integer)
    
    def __repr__(self):
        return "<Stat(roundid='%s', GUID='%s', GUID='%s' , team='%s')>" % (self.round_id, self.GUID, self.alias, self.team)

class WeaponsTable(Base):
    __tablename__ = 'weapons'

    round_id = Column(Integer, primary_key=True)
    GUID = Column(String(30), primary_key=True)
    alias = Column(String(30))
    weapon = Column(String(30), primary_key=True)
    kills = Column(Integer)
    deaths = Column(Integer)
    headshots = Column(Integer)
    hits = Column(Integer)
    shots = Column(Integer)
    
    def __repr__(self):
        return "<Weapon(roundid='%s', GUID='%s', GUID='%s' , team='%s')>" % (self.round_id, self.GUID, self.alias, self.weapon)

Base.metadata.create_all(engine)

