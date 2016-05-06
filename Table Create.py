__author__ = 'matthew.inwood'

import sqlite3
import requests
import json
#Initialize SQLite DB connection.
conn = sqlite3.connect('LolPy.db')
c=conn.cursor()

def createRawDB():
    participantTable = '''create table participant(
      matchID INTEGER not null,
      participantID INTEGER not null,
      teamID INTEGER not null,
      championID INTEGER not null,
      spell1 INTEGER not null,
      spell2 INTEGER not null,
      Lane VARCHAR(8) not null,
      Role VARCHAR(16),
      PRIMARY KEY (matchID,participantID)
      FOREIGN KEY (championID) references champion(championID)
      FOREIGN KEY (spell1) references spells(spellID)
      FOREIGN KEY (spell2) references spells(spellID)
      FOREIGN KEY (teamID) references matches(teamID)
      ) ;'''
    participantTimelineTable = '''create table participantTimeline(
      matchID INTEGER not null,
      participantID INTEGER not null,
      metric VARCHAR(64) not null,
      frame VARCHAR(16) not null,
      value REAL not null,
      PRIMARY KEY (matchID, participantID, metric, frame)
      FOREIGN KEY (matchID) references participant(matchID)
      FOREIGN KEY (participantID) references participant(participantID)
      ) ;'''
    matchHeaderTable = '''create table matches(
      matchID INTEGER not null,
      teamID INTEGER not null,
      Win VARCHAR(8) not null,
      PRIMARY KEY (matchID, teamID)
    ) ;'''
    eventsTable = ''' create table events(
      matchID INTEGER not null,
      time_stamp INTEGER not null,
      eventType VARCHAR(32) not null,
      participantID INTEGER not null,
      no_participants INTEGER,
      attribute VARCHAR(32),
      attribute2 VARCHAR(32),
      xloc INTEGER,
      yloc INTEGER,
      participation VARCHAR(8),
      PRIMARY KEY(matchID,participantID,time_stamp,eventType)
     ) ;'''
    championTable = '''create table champions(
      championID INTEGER not NULL,
      championName VARCHAR(32),
      resource VARCHAR(32),
      PRIMARY KEY (championID)
      ) ;'''
    spellTable = '''create table spells(
      spellID INTEGER not null,
      spellName VARCHAR(32) not null,
      PRIMARY KEY (spellID)
      ) ;'''
    itemsTable = '''create table items(
    itemID INTEGER not null,
    itemName VARCHAR (64),
    goldtotal INTEGER not null,
    goldbase INTEGER not null,
    depth INTEGER not null,
    PRIMARY KEY (itemID)
      ) ;'''
    itemtagsTable = '''create table itemtags(
    itemID INTEGER not NULL,
    tag VARCHAR(32)
      ) ;'''
    champtagsTable = '''create table champtags(
    championID INTEGER not NULL,
    tag VARCHAR(20)
      ) ;'''
    #Drops tables if they exist to refresh static data
    c.execute("DROP TABLE IF EXISTS champtags")
    c.execute("DROP TABLE IF EXISTS participantTimeline")
    c.execute("DROP TABLE IF EXISTS itemtags")
    c.execute("DROP TABLE IF EXISTS items")
    c.execute("DROP TABLE IF EXISTS spells")
    c.execute("DROP TABLE IF EXISTS champions")
    c.execute("DROP TABLE IF EXISTS events")
    c.execute("DROP TABLE IF EXISTS matches")
    c.execute("DROP TABLE IF EXISTS participant")

    c.execute(champtagsTable)
    c.execute(championTable)
    c.execute(itemtagsTable)
    c.execute(itemsTable)
    c.execute(spellTable)
    c.execute(eventsTable)
    c.execute(matchHeaderTable)
    c.execute(participantTable)
    c.execute(participantTimelineTable)

def loaditems():
    input = []
    items = requests.get("https://global.api.pvp.net/api/lol/static-data/na/v1.2/item?itemListData=depth,gold,groups,tags&api_key=4571b727-d76d-4192-9c67-87c441da001c").json()
    for item in items['data']:
        try:
            input.append([items['data'][item]['id'], items['data'][item]['name'], items['data'][item]['gold']['total'],
                  items['data'][item]['gold']['base'], items['data'][item]['depth']])
        except:
            input.append([items['data'][item]['id'], items['data'][item]['name'], items['data'][item]['gold']['total'],
                  items['data'][item]['gold']['base'], 0])
    c.executemany("INSERT INTO items VALUES(?,?,?,?,?)", input)
    conn.commit()
    input = []
    for item in items['data']:
        try:
            for tag in items['data'][item]['tags']:
                input.append([items['data'][item]['id'], tag])
        except:
            continue
    c.executemany("INSERT INTO itemtags VALUES(?,?)", input)
    conn.commit()


def loadchamps():
    champs = requests.get("https://global.api.pvp.net/api/lol/static-data/na/v1.2/champion?champData=partype,tags&api_key=4571b727-d76d-4192-9c67-87c441da001c").json()
    input = []
    for champ in champs['data']:
        input.append([champs['data'][champ]['id'], champs['data'][champ]['name'], champs['data'][champ]['partype']])
    c.executemany("INSERT INTO champions VALUES(?,?,?)", input)
    conn.commit()
    input = []
    for champ in champs['data']:
        for tag in champs['data'][champ]['tags']:
            input.append([champs['data'][champ]['id'], tag])
    c.executemany("INSERT INTO champtags VALUES (?,?)", input)
    conn.commit()

def loadspells():
    input = []
    spells = requests.get("https://global.api.pvp.net/api/lol/static-data/na/v1.2/summoner-spell?api_key=4571b727-d76d-4192-9c67-87c441da001c").json()
    for spell in spells['data']:
        input.append([spells['data'][spell]['id'],spells['data'][spell]['name']])
    c.executemany("INSERT INTO spells VALUES(?,?)", (input))
    conn.commit()

def reloadallstatic():
    championTable = '''create table champions(
      championID INTEGER not NULL,
      championName VARCHAR(20),
      resource VARCHAR(20),
      PRIMARY KEY (championID)
      ) ;'''
    spellTable = '''create table spells(
      spellID INTEGER not null,
      spellName VARCHAR(20) not null,
      PRIMARY KEY (spellID)
      ) ;'''
    itemsTable = '''create table items(
    itemID INTEGER not null,
    itemName VARCHAR (40),
    goldtotal INTEGER not null,
    goldbase INTEGER not null,
    depth INTEGER not null,
    PRIMARY KEY (itemID)
      ) ;'''
    itemtagsTable = '''create table itemtags(
    itemID INTEGER not NULL,
    tag VARCHAR(20)
      ) ;'''
    champtagsTable = '''create table champtags(
    championID INTEGER not NULL,
    tag VARCHAR(20)
      ) ;'''
    c.execute("DROP TABLE IF EXISTS champtags")
    c.execute("DROP TABLE IF EXISTS itemtags")
    c.execute("DROP TABLE IF EXISTS items")
    c.execute("DROP TABLE IF EXISTS spells")
    c.execute("DROP TABLE IF EXISTS champions")
    c.execute(champtagsTable)
    c.execute(championTable)
    c.execute(itemtagsTable)
    c.execute(itemsTable)
    c.execute(spellTable)
    loadspells()
    loaditems()
    loadchamps()



#Run/uncomment these first functions if creating the DB for the first time.
# createRawDB()
# loaditems()
# loadchamps()
# loadspells()

#The reload function is for a DB that needs to refresh static data due to metagame changes,
#new static data (items, spells), or changes to the same. Adds new champions as well.
# reloadallstatic()