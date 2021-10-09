import boto3
import logging
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
            
def get_empty_real_names(ddb_table, min_games = 5):
    pk = "player"
    skname = "sk"
    begins_with = "playerinfo#"
    response = ddb_table.query(KeyConditionExpression=Key('pk').eq(pk) & Key(skname).begins_with(begins_with),
                               FilterExpression=Attr('real_name').not_exists(),
                               ProjectionExpression="sk")
    
    if response['Count'] > 0:
        for record in response["Items"]:
            guid = record["sk"].replace("playerinfo#", "")
            if guid not in ["","1"]:
                get_last_few_aliases(ddb_table, guid, min_games)


def get_last_few_aliases(ddb_table, guid, min_games):
    pk = "player"
    skname = "sk" 
    begins_with = "aliases#" + guid
    response = ddb_table.query(KeyConditionExpression=Key('pk').eq(pk) & Key(skname).begins_with(begins_with))
    debug = False
    
    if response['Count'] > 0:
        alias_counter = Counter()
        for alias in response["Items"]:
            alias_value = alias["data"]
            alias_counter[alias_value] +=1
        if sum(alias_counter.values()) > min_games:
            print("-------")
            print("Processing guid: " + guid)
            print(alias_counter.most_common(6))
            alias_most_common = alias_counter.most_common(10)[0][0]
            print(f'        "{guid}": "{alias_most_common}",') 
        elif debug:
            print("Not enough aliases(" + str(sum(alias_counter.values())) + ") for " + guid)
    elif debug:
        print("Not enough aliases(0) for " + guid)

def get_empty_real_names2(ddb_table, min_games = 5):
    
    index_name = "gsi1"
    pkname = "gsi1pk" 
    # skname = "gsi1sk" 
    
    pk = "realname"
    response = ddb_table.query(IndexName=index_name, KeyConditionExpression=Key(pkname).eq(pk),
                               # & Key(skname).begins_with(begins_with),
                               FilterExpression=Attr('data').not_exists(),
                               ProjectionExpression="pk")
    
    if response['Count'] > 0:
        for record in response["Items"]:
            guid = record["pk"].replace("player#", "")
            if guid not in ["","1"]:
                get_last_few_aliases2(ddb_table, guid, min_games)


def get_empty_real_names3(ddb_table, min_games = 5):
    
    index_name = "gsi1"
    pkname = "gsi1pk" 
    # skname = "gsi1sk" 
    
    pk = "realname"
    response = ddb_table.query(IndexName=index_name, KeyConditionExpression=Key(pkname).eq(pk),
                               # & Key(skname).begins_with(begins_with),
                               FilterExpression=Attr('data').not_exists(),
                               ProjectionExpression="pk")
    
    if response['Count'] > 0:
        for record in response["Items"]:
            guid = record["pk"].replace("player#", "")
            if guid not in ["","1"]:
                get_last_few_aliases2(ddb_table, guid, min_games)

def get_last_few_aliases2(ddb_table, guid, min_games):
    pk = "aliases#" + guid
    response = ddb_table.query(KeyConditionExpression=Key('pk').eq(pk))
    debug = False
    
    if response['Count'] > 0:
        alias_counter = Counter()
        for alias in response["Items"]:
            alias_value = alias["data"]
            alias_counter[alias_value] +=1
        if sum(alias_counter.values()) > min_games:
            print("-------")
            print("Processing guid: " + guid)
            print(alias_counter.most_common(6))
            alias_most_common = alias_counter.most_common(10)[0][0]
            print(f'        "{guid}": "{alias_most_common}",') 
        elif debug:
            print("Not enough aliases(" + str(sum(alias_counter.values())) + ") for " + guid)
    elif debug:
        print("Not enough aliases(0) for " + guid)        
        
def get_last_few_aliases3(ddb_table, guid, min_games):
    pk = "aliases"
    skname = "sk" 
    begins_with = guid
    response = ddb_table.query(KeyConditionExpression=Key('pk').eq(pk) & Key(skname).begins_with(begins_with))
    debug = False
    
    if response['Count'] > 0:
        alias_counter = Counter()
        for alias in response["Items"]:
            alias_value = alias["data"]
            alias_counter[alias_value] +=1
        if sum(alias_counter.values()) > min_games:
            print("-------")
            print("Processing guid: " + guid)
            print(alias_counter.most_common(6))
            alias_most_common = alias_counter.most_common(10)[0][0]
            print(f'        "{guid}": "{alias_most_common}",') 
        elif debug:
            print("Not enough aliases(" + str(sum(alias_counter.values())) + ") for " + guid)
    elif debug:
        print("Not enough aliases(0) for " + guid)

def update_player_info_real_name2(ddb_table, real_name_dict):
    """Update existing players with new real_name."""
    # Example: ddb_table.update_item(Key=Key, UpdateExpression="set elos.#eloname = :elo, elos.#gamesname = :games", ExpressionAttributeNames={"#eloname": "na#6#elo", "#gamesname": "na#6#games"}, ExpressionAttributeValues={':elo': 134, ':games' : 135})
    update_expression="set #data_value = :real_name, lsipk = :lsipk"
    for guid, real_name in real_name_dict.items():
        key = { "pk": "player"+ "#" + guid, "sk": "realname" }
        lsipk = "realname#" + real_name
        expression_values = {':real_name': real_name, ':lsipk': lsipk}
        response = ddb_table.update_item(Key=key, 
                                         UpdateExpression=update_expression, 
                                         ExpressionAttributeValues=expression_values,
                                         ExpressionAttributeNames={"#data_value": "data"})
        if response["ResponseMetadata"]['HTTPStatusCode'] == 200:
            logger.info("Updated name for " + guid + " as " + real_name)
        else:
            logger.warning("Unexpected HTTP code" + str(response["ResponseMetadata"]['HTTPStatusCode']) + ". Did not update " + guid + " " + real_name)           
          

if __name__ == "__main__":
    real_name_dict4 = {
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
    real_name_dict3 = {
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
    
    real_name_dict2 = {
        "fc11488047c7ccf07d3b667b4ade00d6": "kris",
        "fbe2ed832f8415efbaaa5df10074484a": "jam",
        "fba16f435d3bd6973aed9d3449fd1a05": "carnage",
        "fae5bff63ab34b2cbe6db4b1423b7c77": "fonze",
        "fa96d4a163ed3f9c2a3983bc964e828e": "zenix",
        "f8e8ae804becc55b321a043e5a2896e8": "syl",
        "f8637c71d8e5a2b0feab511e7382445b": "mystx",
        "f4d8915ae098c62e5adb1ff27851384f": "plaz",
        "ef2041bac90bddbc7e866287c77a04d2": "elsa",
        "eafea804ea9191f4dc957d009dac0fec": "neo",
        "e84126df87d5c76f4b909ef14403a15b": "sixers",
        "e626a83825acc3528661ae3ebd11bd20": "steve",
        "e04cac87f458048a5d53b6f8813bc910": "zed",
        "ddd3378ee7deb6f93ed2e170c2f4c654": "lasher",
        "dcf1772db8c1c1cef21b2ce8cb4e30ae": "ipod",
        "d4d4bd719b5992ad340694bc48ed001e": "cliffdark",
        "d417e300957bdc39a81540b18e644555": "d3v",
        "ce811d1e9a81d4077276221d559d7b21": "eternal",
        "01cf12e8114d67c45142e0f62cd24628": "nigel",
        "029568b3f1a6be2a3a0abc2801132f75": "backmagik",
        "05bf7407f67bd9bf77fb5a0c0a37aa89": "virus047",
        "092b932c7ff15681c699ff3e587bb30e": "blazk",
        "098f6bcd4621d373cade4e832627b4f6": "murkey",
        "0d25a81785f7a2c03590410994884053": "olden",
        "10352b3aab1331dafbc842131738f874": "kiz",
        "10696de558af19f8599a48f18705691b": "fro",
        "111a77f3fed13d7ee45579489a922c90": "malmen",
        "13f0f65682c3d85121d8b654cc7d707b": "packiej",
        "14ac583287528b0a5b1b0a5121edb0b7": "eddo",
        "18c4c31131ec8067a0bbb9c18e360468": "lrd",
        "19cf2c20f150c255911d7c01caac05e2": "adlad",
        "1e24524f380985623ec9f215a409b1b6": "siluro",
        "22b0e88467093a63d5dd979eec2631d1": "donkz",
        "23b2521206ae62b595c4a78de79aa8f9": "yeniceri",
        "2b7a74823eb4f1a2a4ea54a7366b811a": "merlinator",
        "383be354f366e213fad3fec6f2a77d39": "nizou",
        "3e73ede96b4607286a753e41631ce076": "delgon",
        "432626a8732c15a42910c231fa9cbe62": "dictator",
        "43a2cf49f0dcd228cc9f3dfcb27df158": "biggi",
        "5379320f3c64f43cdaf3350fc13011ce": "xrb",
        "58e419de5a8b2655f6d48eab68275db5": "faster",
        "65f39437a7455f6b94c9e999dfbb060d": "rob",
        "6619a6fdc2ebb392371a28c68ce3cab3": "mata",
        "677560c5fc004980e846911049ce3ee7": "mirage",
        "69a89ff1b245d666324acf899c87bb74": "engma",
        "69aeab8db8aba4814b86853101f5b528": "lelle",
        "6a5be834b6d2fc5789d482e5fa6b84f6": "murkey",
        "6ae623d02db90921de9ede71ada85351": "silentstorm",
        "6afd85307c0bdf40c2e8c13ce1c4d7b6": "toryu",
        "7058a686841807d4c20af6730ea823ab": "joep",
        "72a5d6513a00f86bacf62e95583f4959": "leonneke",
        "7700b9b2b70bb2c48b71fe2be571c8bf": "rekernator",
        "80eb428d71a1d11c65acbc42ebcf2a39": "miles",
        "8879a24a9769680f3bacf3bd485dbace": "syn",
        "8cb6576202ecfb1fb9587f0425a1108f": "dary",
        "8ff4ecf7bd1b87edad5383efcfdb3c8d": "illkilla",
        "905a6fd997a70fe18309ea3030c7bc7c": "tragic",
        "90d02d2fa11f37f62c6f624109d87ab5": "mooshu",
        "9524f4278ac802d005fd42e4ba3c0a1d": "luna",
        "9562e8ae4aed6279c5e4b1592e23be3c": "festus",
        "96a8ea1acd085303c5267232e271648f": "corpse",
        "96aafa402b8e44cb887c79e047f0e772": "parcher",
        "a928daaba6dbdb67ba9b392c3966b22e": "source",
        "a96cd62f89fb020a468263568bca83b2": "snipercat",
        "b2d502943ddae6108f1ff22bc09e3367": "twister",
        "b916224dc1cbeecae3cef184a1af44ba": "vacs",
        "41e30e5dd230f4469712df0f4c3e60c3": "naif(/)",
        "c4bee65ef1516ea08bac14d6a0be00ab": "gracoes",
        "c0ab6495072452985c7bdf6657493a22": "dtto",
        "bcbf3c6eefa4d8a7685d46c826804d0d": "ding",
        }
    real_name_dict1 = {
        "c4bee65ef1516ea08bac14d6a0be00ab": "gracoes",
        "f50e7de0a7deefbb088c60c0d89a79eb": "webe",
        "fb4b4ae08d99d0d5d798aa5530a982eb": "owzo",
        "fd7bb250e8bb1a33350bc0f9034d4d3c": "np.l4mpje",
        "f235396f1069b50cbd90908636112135": "bloodje",
        "f0f878b447747af6fc2045bcfa68d2ce": "crumbs",
        "ecfc385510bbbaa564f8b6cfd4c68f61": "kittens",
        "e568ef7afff4595eb4f2a39af8e589ff": "kengog",
        "e36178cfe46d84198b5f369b920f9592": "souldriver",
        "e126a9586a4b96fff9438a776975afb9": "bully",
        "da07b82953745515f40af018d02be094": "bru",
        "d6f9fe03c50084062041d8fc11be57ed": "toxin",
        "cddaa8f726bdbf1f054fa76237507679": "sem",
        "c835c658572c4c974fc82f3f2ee2799b": "vis",
        "c5e33e4d09c2cba378f105ed64c0ed9b": "d1ego",
        "c5b5d6eddfa435d61b42c9af1c1fba74": "cky",
        "c22dc4b078b2eb6ccc9b3094560d3ebf": "prowler",
        "be5eed7e4717d442e2fe9c3e90e3c51f": "pipe",
        "b3465bff43fe40ea76f9e522d3314809": "parcher",
        "b1ae78b449ff114b976f38a738c8b784": "tosspot",
        "a1153397a7b333078b710e7ec535f364": "uyop",
        "a0b20c2b0f9febc038b001260907185a": "cypher",
        "96fe9714e6041eb1c8b2c01e2c06f63b": "conscious",
        "820b6c493fa3b7d3b577efcda612991c": "stahl",
        "7d669f9c93ac1bcfe05e5b6553ade0bb": "whizzard",
        "7d57743b60c7b7b10a62fc7b2e00bc0d": "je$tz0r",
        "7d52b9640302f4d469211cc075ce5057": "john_mullins",
        "7d17eafdb49fd5577974ad143da515bc": "dillweed",
        "7c6a3fd4c8ed8f4b2e1638d1936ae959": "utrolig",
        "7148bf623a507de5c5d400cd0e9cf2b4": "oksii",
        "64FDC59648F4C7": "nigel",
        "611d79d1b08949e650c4c299aadd0fde": "spaztik",
        "5ddda39a8d1e8f685ca77a0b232dc565": "raiser",
        "572e2202f20279063d9e6da7418b0c62": "swanidius",
        "54128439647bfa229bd36be3a7c36ea4": "warri0r",
        "5377b0fe4286aee69fc4c49d2d44c295": "c@k-el",
        "4cdd977998c99627b48a323248f63edf": "sonix",
        "49d64b3c0fcd5512a8e87b6287b29b21": "xill",
        "477c523b105a795728682ee27a678db7": "v1rkes",
        "3654a03f0b140222b475dc809a56ce20": "dusty",
        "36366e2f47dd2b3ec445848dd29b47c8": "silentstorm",
        "34e4207e8320e98830e65224bb27a6eb": "oranje",
        "2c0e52c26f06ab91954a8f2e49939a73": "ducts",
        "2b948fb19e6fdabc48fe3616b7070c21": "blox",
        "1d2d182cba337487db76d6ba02b76c18": "taby",
        "11925ec75862feb61ebf9d659784f6a4": "die",
        "1177bf7dcacebac3885a56d01524df3c": "donkey",
        "10d3f973708df6a3265b3f8be6d1ce74": "brako",
        "09e74b17f0910bca98d523305670548b": "goose",
        }
    real_name_dict = {
        "08ce652ba1a7c8c6c3ff101e7c390d20": "kevin",
        "08dce4247a598442bb5a3acf2223e5c0": "canhanc",
        "3b574d914d9bef5dc61dd3438af31b2f": "nova",
        "8e6a51baf1c7e338a118d9e32472954e": "doza",
        "a8cbc2fb87e7e1191be0f87637473e8b": "iii",
        "b6b61456699bd30b48fe6c1249efeab9": "jin",
        "CD0BB1C6361C5D": "valor-jaytee",
        "80fc34f8c0cdd139e3e85e143cbe9f3b": "rezta",
        "65584f6d88375109b3c15e525c3435b6": "diegosky",
        "68deaefc0a07be79fcb2cc5104a71b1e": "crabje",
        "965098f826dc9ec2ac14de6949698408": "s1lentnoob",
        "901a69f8ea850ff932aaeb0cb6c25dec": "shaz",
        } # complete
    
    read_only = True
    if read_only:
        get_empty_real_names2(ddb_table, min_games = 4)
    else:
        update_player_info_real_name(ddb_table, real_name_dict)


