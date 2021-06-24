import boto3
import logging
import json
import os

# for local testing use actual table
# for lambda execution runtime use dynamic reference
TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"


dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('real_name_bkp')
logger.setLevel(log_level)

log_stream_name = "local"

def update_player_info_real_name(ddb_table, real_name_dict):
    """Update existing players with new real_name."""
    # Example: ddb_table.update_item(Key=Key, UpdateExpression="set elos.#eloname = :elo, elos.#gamesname = :games", ExpressionAttributeNames={"#eloname": "na#6#elo", "#gamesname": "na#6#games"}, ExpressionAttributeValues={':elo': 134, ':games' : 135})
    update_expression="set real_name = :real_name, lsipk = :lsipk"
    for guid, real_name in real_name_dict.items():
        key = { "pk": "player" , "sk": "playerinfo#" + guid }
        lsipk = "playerinfo#" + real_name + "#" + guid
        expression_values = {':real_name': real_name, ':lsipk': lsipk}
        response = ddb_table.update_item(Key=key, 
                                         UpdateExpression=update_expression, 
                                         ExpressionAttributeValues=expression_values)
        if response["ResponseMetadata"]['HTTPStatusCode'] == 200:
            logger.info("Updated name for " + guid + " as " + real_name)
        else:
            logger.warning("Unexpected HTTP code" + str(response["ResponseMetadata"]['HTTPStatusCode']) + ". Did not update " + guid + " " + real_name)           
            


if __name__ == "__main__":
    real_name_dict = {
        "096EAA64064398": "nizouuuu",
        "10DDD781C9AB87": "Stahl",
        "1299E6698A2B37": "bru",
        "135B29DA9DB755": "vodka",
        "1441314A80B76F": "kittens",
        "1AA8A92BB21F83": "grit",
        "1BE7B9F0E29EAD": "ra!ser",
        "1CEEDAB80C53DD": "corpse",
        "2052FC6191C31F": "pop",
        "23A888044AAD96": "spaztik",
        "25AFFD9E8C0F40": "miles",
        "2918F80471E175": "donka",
        "291C139A747E9D": "rev9",
        "2F06073C3DB647": "pingrage",
        "389BFC5BBC7722": "kazz",
        "39D86514D9A949": "magik",
        "3A5DE472B948F3": "parcher",
        "3C02715F54A3A1": "fro",
        "44D0B08B78E9F2": "c@k-el",
        "4647C6E3D9B295": "gut",
        "519B97F53B116D": "brandon",
        "51A814AF4FAB03": "v1rkes",
        "52162197296741": "elsa",
        "562968C29CC44A": "druwin",
        "57A0215AB16B9A": "fistermiagi",
        "5CB7908DEAF2FB": "ca$hh",
        "7712A8C0E06701": "doza",
        "7789BAB9C4D99F": "canhanc",
        "8EE5313A2172CD": "mooshu",
        "934D5F55971F99": "prwlr",
        "980A27C4F9E48F": "pipe",
        "991934432F48CC": "mmb!rd",
        "A53B3ED2A896CB": "parcher",
        "A795F1760BF4DF": "luna",
        "AAF904F9E26000": "h2o",
        "A53B3ED2A896CB": "parcher",
        "B1EDB917E888CC": "dillweed",
        "BA285E4C293F16": "joep",
        "BD6A1037FC1621": "krazykaze",
        "C16F54F4CB35C3": "joep",
        "C241209BDE5BB0": "scrilla",
        "C544D92BEA578A": "prwlr",
        "CB7BC75B081B0E": "rob",
        "CBF7EA94F52F1D": "festus",
        "E79B8A18A9DBB7": "john_mullins",
        "EB967561E5518C": "krazykaze",
        "ED47639F9DE20F": "mooshu",
        "FDE32828995C0A": "fonze",
        "FFF4EF1FD439BB": "source"
    }
    update_player_info_real_name(ddb_table, real_name_dict)


