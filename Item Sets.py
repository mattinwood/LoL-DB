__author__ = 'matthew.inwood'
import pprint
import requests
import json
import time
import csv
#import pickle
import sqlite3

#Initialize connection to the SQLite Database
conn = sqlite3.connect('LolPy.db')
c=conn.cursor()

def main():
        #players.agg returns a list of all challenger and master players from the API.
        playerIDs = set(_players.agg())
        print(len(playerIDs),": ",playerIDs)
        #gameIDs get all available match IDs for the players from the API
        gameIDs = list(_matches.idFetch(playerIDs))
        #
        gameIDs = _io.gameID(gameIDs)
        eventBatch, headerBatch, participantBatch, participantTimeline = _tableData.matchesData(gameIDs)
        _io.popDB(eventBatch, headerBatch, participantBatch, participantTimeline)

class _players:
    def getChallenger():
        challengerList = requests.get("https://na.api.pvp.net/api/lol/na/v2.5/league/challenger?type=RANKED_SOLO_5x5&api_key={key}".format(key=myKey))
        challengers = json.loads(challengerList.text)
        playerIDList = []
        for i in challengers['entries']:
            playerIDList.append(i['playerOrTeamId'])
        return(playerIDList)
    ##################################
    def getMaster():
        challengerList = requests.get("https://na.api.pvp.net/api/lol/na/v2.5/league/master?type=RANKED_SOLO_5x5&api_key={key}".format(key=myKey))
        challengers = json.loads(challengerList.text)
        playerIDList = []
        for i in challengers['entries']:
            playerIDList.append(i['playerOrTeamId'])
        return(playerIDList)
    ##################################
    def agg():
        allPlayers = []
        for challenger in _players.getChallenger():
            allPlayers.append(challenger)
        for master in _players.getMaster():
            allPlayers.append(master)
        #print(len(allPlayers))
        return(allPlayers)

class _matches:
    def idFetch(players):
        pSet = set()
        i = 0
        j = 0
        for player in players:
            while True:
                try:
                    matchesJ = requests.get("https://na.api.pvp.net/api/lol/na/v2.2/matchlist/by-summoner/{playerID}?rankedQueues=TEAM_BUILDER_DRAFT_RANKED_5x5&api_key={key}".format(playerID=player,key=myKey))
                    matches = json.loads(matchesJ.text)
                    for n in range(len(matches['matches'])):
                        pSet.add(matches['matches'][n]['matchId'])
                        #print(len(pSet),", ",pSet)
                    i += 1
                    time.sleep(2)
                    j = 0
                except:
                    j += 1
                    print("Rate limit exceeded; waiting 15 seconds. Current time is ",time.strftime("%H:%M:%S",time.localtime()))
                    print("Current player location: ", i, " ",j, " times.")
                    time.sleep(5)
                    if j == 5:
                        j = 0
                        pass
                    continue
                break
        return(pSet)
        
class _io:
    def gameID(idfile):
        print(idfile)
        try:
            reader = csv.reader(open('gameID.csv'))
            for line in reader:
                for element in line:
                    idfile.add(int(element))
            print(idfile)
        except:
            pass
        print(type(idfile))
        writer = csv.writer(open('gameID.csv', 'w', newline=''))
        writer.writerow(list(idfile))
        return(list(idfile))

    def popDB(eventBatch, headerBatch, participantBatch, participantTimeline):
        for game in eventBatch:
            #batch insert on this line.
            c.executemany("INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?,?,?)", game)
            conn.commit()

        for game in headerBatch:
            c.executemany("INSERT OR IGNORE INTO matches VALUES (?,?,?)", game)
            conn.commit()

        for game in participantBatch:
            c.executemany("INSERT OR IGNORE INTO participant VALUES (?,?,?,?,?,?,?,?)", game)
            conn.commit()

        for game in participantTimeline:
            c.executemany("INSERT OR IGNORE INTO participantTimeline VALUES (?,?,?,?,?)", game)
            conn.commit()

class _tableData:

    def matchesData(gameIDs):
        i = 0
        j = 0
        eventBatch = []
        headerBatch = []
        participantBatch = []
        participantTimeline = []
        for gameID in gameIDs:
            while True:
                if j == 5:
                    j = 0
                    continue
                try:
                    matchData = requests.get("https://na.api.pvp.net/api/lol/na/v2.2/match/{id}?includeTimeline=true&api_key={key}".format(id=gameID, key=myKey)).json()
                    eventBatch.append(_tableData.eventFetch(matchData))
                    headerBatch.append(_tableData.headerFetch(matchData))
                    participantBatch.append(_tableData.participantFetch(matchData))
                    participantTimeline.append(_tableData.participantTimelineFetch(matchData))
                    i += 1
                    time.sleep(1)
                except:
                    j += 1
                    print("Rate limit exceeded; waiting 15 seconds. Current time is ",time.strftime("%H:%M:%S",time.localtime()))
                    print("Processed ", i, " keys. Skipped ", j, " times.")
                    time.sleep(5)
                break
        return(eventBatch, headerBatch, participantBatch, participantTimeline)

    def headerFetch(gameIDs):
        headerBatch = []
        for i in gameIDs['teams']:
            record = gameIDs['matchId'],i['teamId'],i['winner']
            headerBatch.append(record)
        return(headerBatch)

    def participantFetch(gameIDs):
        participantBatch = []
        for i in gameIDs['participants']:
            record = (gameIDs['matchId'], i['participantId'], i['teamId'],i['championId'],i['spell1Id'],i['spell2Id'],i['timeline']['lane'],i['timeline']['role'])
            participantBatch.append(record)
        return(participantBatch)

    def eventFetch(timeline):
        batch = []
        for i in timeline['timeline']['frames']:
            try:
                for event in i['events']:
                    if event['eventType'] == 'BUILDING_KILL':
                        if event['buildingType'] == 'TOWER_BUILDING':
                            try:
                                record = [timeline['matchId'], event['timestamp'], event['eventType'], event['killerId'],
                                          len(event['assistingParticipantIds']), event['towerType'], event['laneType'],
                                          event['position']['x'], event['position']['y'],'Kill']
                                batch.append(record)
                                for assist in event['assistingParticipantIds']:
                                    record = [timeline['matchId'], event['timestamp'], event['eventType'], assist,
                                              len(event['assistingParticipantIds']), event['towerType'], event['laneType'],
                                              event['position']['x'], event['position']['y'],'Assist']
                                    batch.append(record)
                            except:
                                record = [timeline['matchId'], event['timestamp'], event['eventType'], event['killerId'],
                                          0, event['towerType'], event['laneType'],
                                          event['position']['x'], event['position']['y'],'Kill']
                                batch.append(record)
                        elif event['buildingType'] == 'INHIBITOR_BUILDING':
                            try:
                                record = [timeline['matchId'], event['timestamp'], event['eventType'], event['killerId'],
                                          len(event['assistingParticipantIds']), 'INHIBITOR', event['laneType'],
                                          event['position']['x'], event['position']['y'],'Kill']
                                batch.append(record)
                                for assist in event['assistingParticipantIds']:
                                    record = [timeline['matchId'], event['timestamp'], event['eventType'], assist,
                                              len(event['assistingParticipantIds']), 'INHIBITOR', event['laneType'],
                                              event['position']['x'], event['position']['y'],'Assist']
                                    batch.append(record)
                            except:
                                record = [timeline['matchId'], event['timestamp'], event['eventType'], event['killerId'],
                                          0, 'INHIBITOR', event['laneType'],
                                          event['position']['x'], event['position']['y'],'Kill']
                                batch.append(record)
                    elif event['eventType'] == 'CHAMPION_KILL':
                        try:
                            record = [timeline['matchId'], event['timestamp'], event['eventType'], event['killerId'],
                                      len(event['assistingParticipantIds']), event['victimId'], 'NULL',
                                      event['position']['x'], event['position']['y'],'Kill']
                            batch.append(record)
                            for assist in event['assistingParticipantIds']:
                                record = [timeline['matchId'], event['timestamp'], event['eventType'], assist,
                                          len(event['assistingParticipantIds']), event['victimId'], 'NULL',
                                          event['position']['x'], event['position']['y'],'Assist']
                                batch.append(record)
                        except:
                            record = [timeline['matchId'], event['timestamp'], event['eventType'], event['killerId'],
                                      0, event['victimId'], 'NULL',
                                      event['position']['x'], event['position']['y'],'Kill']
                            batch.append(record)
                    elif event['eventType'] == 'ELITE_MONSTER_KILL':
                        record = [timeline['matchId'], event['timestamp'], event['eventType'], event['killerId'],
                                  'NULL', event['monsterType'], 'NULL',
                                  event['position']['x'], event['position']['y'],'Kill']
                        batch.append(record)
                    elif event['eventType'] == 'ITEM_DESTROYED':
                        record = [timeline['matchId'], event['timestamp'], event['eventType'], event['participantId'],
                                  'NULL', event['itemId'], 'NULL',
                                  'NULL', 'NULL','NULL']
                        batch.append(record)
                    elif event['eventType'] == 'ITEM_PURCHASED':
                        record = [timeline['matchId'], event['timestamp'], event['eventType'], event['participantId'],
                                  'NULL', event['itemId'], 'NULL',
                                  'NULL', 'NULL','NULL']
                        batch.append(record)
                    elif event['eventType'] == 'ITEM_SOLD':
                        record = [timeline['matchId'], event['timestamp'], event['eventType'], event['participantId'],
                                  'NULL', event['itemId'], 'NULL',
                                  'NULL', 'NULL','NULL']
                        batch.append(record)
                    elif event['eventType'] == 'ITEM_UNDO':
                        if(event['itemAfter']) != 0:
                            record = [timeline['matchId'], event['timestamp'], event['eventType'], event['participantId'],
                                      'NULL', event['itemBefore'], 'NULL',
                                      'NULL', 'NULL','NULL']
                            batch.append(record)
                    elif event['eventType'] == 'SKILL_LEVEL_UP':
                        record = [timeline['matchId'], event['timestamp'], event['eventType'], event['participantId'],
                                  'NULL', event['skillSlot'], event['levelUpType'],
                                  'NULL', 'NULL','NULL']
                        batch.append(record)
                    elif event['eventType'] == 'WARD_KILL':
                        record = [timeline['matchId'], event['timestamp'], event['eventType'], event['killerId'],
                                  'NULL', event['wardType'], 'NULL',
                                  'NULL', 'NULL','NULL']
                        batch.append(record)
                    elif event['eventType'] == 'WARD_PLACED':
                        record = [timeline['matchId'], event['timestamp'], event['eventType'], event['creatorId'],
                                  'NULL', event['wardType'], 'NULL',
                                  'NULL', 'NULL','NULL']
                        batch.append(record)
            except:
                #This is needed because there's an initial event datapoint that is meaningless for this code.
                continue
        return(batch)

    def participantTimelineFetch(gameIDs):
        participantTimeline = []
        for i in gameIDs['participants']:
            for key in i['timeline']:
                for frame in i['timeline'][key]:
                    try:
                        record = (gameIDs['matchId'],i['participantId'], key, frame, i['timeline'][key][frame])
                        participantTimeline.append(record)
                    except:
                        continue
        return(participantTimeline)

##########################################################################
pp = pprint.PrettyPrinter(indent=4)
#myKey should be your personal API key from Riot.
myKey = "4571b727-d76d-4192-9c67-87c441da001c"

#Bookkeeping to keep track of runtime. Size of Master and Challenger pools can force
#the program to run for hours/days due to API limits.
begin = time.time()
main()
end = time.time()
print("This program took ",(end-begin)/60," minutes to run.")