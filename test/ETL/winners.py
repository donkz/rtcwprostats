import boto3
import logging
from boto3.dynamodb.conditions import Key
import json
import pandas as pd

# for local testing use actual table
# for lambda execution runtime use dynamic reference
TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
log_stream_name = "local"

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('find_winners')
logger.setLevel(log_level)


pk = "match"
skname = "sk"
begins_with = "16"
limit = 400
response = ddb_table.query(KeyConditionExpression=Key('pk').eq(pk) & Key(skname).begins_with(begins_with),
                           ExpressionAttributeNames={"#data_value": "data"},
                           ProjectionExpression="#data_value",
                           Limit=limit, 
                           ScanIndexForward=False,
                           ReturnConsumedCapacity='TOTAL')
logger.info("Query ConsumedCapacity:" + str(response['ConsumedCapacity']['CapacityUnits']))

matchinfos = {}
'''
0 map
1 r1_time_limit
2 r1_winner
3 r2_time_limit
4 game_winner
'''
columns = ["map","r1_time_limit","r1_winner","r2_time_limit","4 game_winner"]

for match in response["Items"]:
    match_data_json = json.loads(match["data"])
    match_id = match_data_json["match_id"]
    if match_id not in matchinfos:
        matchinfos[match_id] = ["","","","",""]
    if match_data_json["round"] == "1":
        matchinfos[match_id][1] = match_data_json["time_limit"]
        matchinfos[match_id][2] = match_data_json["winner"]
    if match_data_json["round"] == "2":
        matchinfos[match_id][0] = match_data_json["map"]
        matchinfos[match_id][3] = match_data_json["time_limit"]
        matchinfos[match_id][4] = match_data_json["winner"]
        
df = pd.DataFrame.from_dict(matchinfos, orient='index', columns = columns)
df.to_csv("c:\\a\\matches.csv")

    


