import boto3
import logging
import json
import os
from boto3.dynamodb.conditions import Key, Attr
from collections import Counter

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
            
def get_empty_real_names(ddb_table):
    pk = "player"
    skname = "sk"
    begins_with = "playerinfo#"
    response = ddb_table.query(KeyConditionExpression=Key('pk').eq(pk) & Key(skname).begins_with(begins_with),
                               FilterExpression=Attr('real_name').not_exists(),
                               ProjectionExpression="sk")
    
    if response['Count'] > 0:
        for record in response["Items"]:
            guid = record["sk"].replace("playerinfo#", "")
            get_last_few_aliases(ddb_table, guid)


def get_last_few_aliases(ddb_table, guid):
    pk = "player"
    skname = "sk" 
    begins_with = "aliases#" + guid
    response = ddb_table.query(KeyConditionExpression=Key('pk').eq(pk) & Key(skname).begins_with(begins_with))
    if response['Count'] > 0:
        alias_counter = Counter()
        for alias in response["Items"]:
            alias_value = alias["data"]
            alias_counter[alias_value] +=1
        print(alias_counter.most_common(10))
        alias_most_common = alias_counter.most_common(10)[0][0]
        print(f'        "{guid}": "{alias_most_common}",') 

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
    real_name_dict = {
        "0267DFCD0A0C4C": "cliffdark",
        "093BDE87AD00E0": "bully",
        "0ACA00499288FB": "murkey",
        "0F2FA5EFF5771E": "mote",
        "0F709A657ED221": "jimmy",
        "1298A8F01C0B6C": "kris",
        "13466A0F5A89DC": "d|ng",
        "16BBF399E78274": "kep",
        "1ACBE6343DC5BA": "zenix",
        "1CF86C1D4C4E15": "whizz",
        "1D078AA28E2A7A": "mata",
        "206A67CC89B45B": "v1rkes",
        "21C6F443F8F61E": "jimmy",
        "2910277EB373F2": "kiz",
        "291C139A747E9D": "rev9",
        "2F4882C68FEA1F": "ipod",
        "2FEECBB5265DE5": "source",
        "31CC569471C123": "dillweed",
        "323631B5B34201": "mmb!rd",
        "455E772A28D801": "10-pack",
        "4712444127FA3D": "muztee.keytaro",
        "48EE87B375AC16": "[m]die",
        "49E03EB7C0C9C6": "kali",
        "5197824281EDF9": "bonehead",
        "56DE83343F8E18": "nova",
        "57440E9ADD4D44": ".cue",
        "5BE44CB4DB989D": "utrolig",
        "5F27C60BA65823": "snipercat",
        "5F45A6981FEEE8": "lasher",
        "5F801981AB2FBA": "pimp",
        "63D1BA0A77635E": "jallaaaaaa",
        "653ED61DD6C853": "conscious",
        "661701B7A8AD03": "donkey",
        "66A2121F6337E8": "blazk",
        "679AB4FE576923": "mirage",
        "686499A13AB34A": "chileno",
        "6EF15B0B231A1A": "d3v",
        "707481522E63FA": "ipod",
        "71231055BAA4A8": "eddo",
        "72841264BC0392": "merlinator",
        "774228054D8E91": "dictator",
        "783DDD39CD756C": "webe",
        "7DE89DCFE2A22F": "vacs",
        "7e4e0f2b1ce91e": "murkey",
        "822CD1EB6B46BA": "illkilla",
        "82EDF8FC8BFEF7": "carnage",
        "83582974134660": "toryu",
        "8588E9D55C074C": "corpse",
        "86A40E83BD9545": "plaz",
        "8C91B21590C3E8": "lelle!",
        "8FA545D3E4B815": "puma",
        "8FE7B746C7914D": "sengo",
        "91493F4F94B49D": "adlad",
        "9570ADE4A0420C": "askungen:o)",
        "9312D9AC447886": "donka?",
        "9570ADE4A0420C": "askungen",
        "9AF43C6C2100FA": "desk1ciao",
        "9CCFEC037808D3": "eternal",
        "9F69FD9EDEEC2A": "bonehead",
        "A1F86730562542": "owzo",
        "A4BDC3D0FD4E07": "yeniceri",
        "A5AA0050BFBB5C": "vacs",
        "A7AD0CC3699FDE": "delgon",
        "A87B8220E78A16": "desk1ciao",
        "AD1562147DDEF2": "terrrr",
        "B2886CA5A378C3": "biggi",
        "B3D7C36AEBFFA5": "deadeye",
        "B9D262F980C3E6": "mister",
        "BBF8F8B9CB291E": "kye",
        "BF49841E120950": "flogzero",
        "BF9A2DC0AC5FCF": "kam1zama",
        "C4342192B682BB": "dtto",
        "CCC0A9FF7E3656": "sem",
        "CD83F962CF5513": "silentstorm",
        "D07A14C4CD0C5C": "risk",
        "D09264AC5BB2BF": "rob",
        "D184E33AC2CD5B": "crumbs",
        "D6809D025CD614": "resiak",
        "D886FC322C2577": "jam",
        "DB2400DC4A5F94": "ding",
        "DF3B130253459D": "chileno",
        "E66E9FFF7C31A6": "jin",
        "E6D2D226732BA8": "kittens",
        "EB9CA4E0720793": "dary",
        "EDB8C7D23998A3": "enigma",
        "EF720C0BAAAA74": "illkilla",
        "F052A146AF7C6A": "h2o",
        "F1E357A54380EB": "rivendale",
        "F26D2F8CA060CF": "xrb",
        "F4773DA5960518": "faster",
        "FDA55D6797CBBE": "reker",
        "FDA630F9167B17": "leonneke",
    }
    update_player_info_real_name(ddb_table, real_name_dict)


