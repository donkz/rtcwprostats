import requests
import json
import logging

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("testAPI")
logger.setLevel(log_level)

# define Python user-defined exceptions
class Error(Exception):
    """Base class for other exceptions"""
    pass

class ValueTooSmallError(Error):
    """Raised when the input value is too small"""
    pass

class ValueTooLargeError(Error):
    """Raised when the input value is too large"""
    pass

class ValueIncorrectSize(Error):
    """Raised when the input value is too large"""
    pass

def get_api_response_as_json(url_path_match):
    url_api = 'https://rtcwproapi.donkanator.com/'
    url = url_api + url_path_match
    logger.info("Retrieving " + url)
    response = requests.get(url)
    logger.info(response.text[0:80])
    result_json = json.loads(response.text)
    if "error" in result_json:
        logger.error(result_json["error"])
    if "Internal server error" in response.text:
        raise ValueError("API call failed to be processed by the back end.")
    return result_json

#i should be writing real test framework, but for now this will do
def check_obj_type(obj, correct_type):
    if not isinstance(obj, correct_type):
        logger.error("Object is not what is expected.")
        raise TypeError
        
def check_num_elements(obj, desired_minimum, desired_maximum):
    if len(obj) < desired_minimum:
        logger.error("Length of the result is too small")
        raise ValueTooSmallError
    if len(obj) > desired_maximum:
        logger.error("Length of the result is too big")
        raise ValueTooLargeError

def check_json_value(obj, key, correct_type, length):
    if key not in obj:
        logger.error("Key " + key + " was not found.")
        raise KeyError
    if not isinstance(obj[key], correct_type):
        logger.error(key + " is not the type as expected.")
        raise KeyError
    if length:
        if len(str(obj[key])) != length:
            logger.error(key + " is not not of expected length.")
            raise KeyError

test_matches = ['1610076805']
match_id_array = "1630476331,1630475541,1630474233,1630472750"
test_guids = ['22b0e88467093a63d5dd979eec2631d1']
seach_player = "donk"
server_name = "^dS^1A^7|RTCWCHILE.COM"
region = "na"

match_id = test_matches[0]
player_guid = test_guids[0]

# /matches/single_match
url_path_match = "matches/" +  match_id + "2"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 1)
check_json_value(obj[0], "match_id", str, 10)

# /matches/several_matches
url_path_match = "matches/" +  match_id + "1,"+  match_id + "2"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 2, 2)
check_json_value(obj[0], "match_id", str, 10)

# /matches/recent
url_path_match = "matches/recent"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 2, 100)
check_json_value(obj[0], "match_id", str, 10)

# /matches/recent/number
url_path_match = "matches/recent/92"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 10, 100)
check_json_value(obj[0], "match_id", str, 10)

# /stats/player/{player_guid}
url_path_match = "stats/player/" + player_guid
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 0, 40)
if len(obj) > 0:
    check_json_value(obj[0], "match_id", str, 10)
    check_json_value(obj[0], "categories", dict, None)
    check_json_value(obj[0], "alias", str, None)
else:
    logger.warning("No player stats in last 30 days. Skipped integrity checks.")

# /wstats/player/{player_guid}
url_path_match = "wstats/player/" + player_guid
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 0, 40)
if len(obj) > 0:
    check_json_value(obj[0], "match_id", str, 10)
    check_json_value(obj[0], "wstats", list, None)
else:
    logger.warning("No player wstats in last 30 days. Skipped integrity checks.")

# /stats/{match_id}
url_path_match = "stats/" +  match_id
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 3, 30)
check_json_value(obj, "match_id", str, 10)

# /stats/{match_id} with csv
url_path_match = "stats/" +  match_id_array
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 4, 4)
# check_json_value(obj[0], "1630476331", list, None)

# /wstats/{match_id}
url_path_match = "wstats/" +  match_id
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 2, 2)
check_json_value(obj, "match_id", str, 10)
check_json_value(obj, "wstatsall", list, None)

# /wstats/player/{player_guid}/match/{match_id}
match_id_local = "1627011217"
url_path_match = "wstats/player/" + player_guid + "/match/" + match_id_local
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 2, 3)
check_json_value(obj, "match_id", str, 10)
check_json_value(obj, "wstats", list, None)
check_json_value(obj, "player_guid", str, 32)

# /gamelogs/{match_round_id}"
url_path_match = "gamelogs/" +  match_id + "2"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 4, 3000)
check_json_value(obj[0], "unixtime", str, 10)

# /player/{player_guid}
url_path_match = "player/" +  player_guid
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 4, 30)
check_json_value(obj, "real_name", str, None)

# /player/search/{begins_with}
url_path_match = "player/search/" +  seach_player
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 30)
check_json_value(obj[0], "real_name", str, 5)

# /match/{server}
url_path_match = "matches/server/" +  server_name
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 30)
check_json_value(obj[0], "round", str, 1)

# /matches/type/{region}/{teams}
url_path_match = "matches/type/" +  "na/6"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 101)
check_json_value(obj[0], "round", str, 1)

#expecting error
url_path_match = "matches/type/" +  "mx"
logger.info("Expecting error.")
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 1, 1)
check_json_value(obj, "error", str, None)

#expecting error
url_path_match = "matches/type/" +  "na/8"
logger.info("Expecting error.")
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 1, 1)
check_json_value(obj, "error", str, None)

# /matches/type/{region}/{teams}
url_path_match = "matches/type/" +  "na/6"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 101)
check_json_value(obj[0], "round", str, 1)

# group retrieve vars
region = "na"
match_type = "6"
group_name = "gather15943"
# /groups/group_name/{group_name}
url_path_match = "groups/group_name/" +  group_name
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 1, 10)
check_json_value(obj, "gather15943", list, None)

# /groups/region/{region_name}
url_path_match = "groups/region/" +  region
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 1, 100)
#check_json_value(obj, "gather15943", list, None)

# /groups/region/{region_name}/type/{match_type}
url_path_match = "groups/region/" +  region + "/type/" + match_type
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 1, 100)

# /groups/region/{region_name}/type/{match_type}/group_name/{group_name}
url_path_match = "groups/region/" +  region + "/type/" + match_type + "/group_name/" + group_name
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 1, 10)
check_json_value(obj, "gather15943", list, None)

# Expecting error
url_path_match = "groups/region/" +  region + "/type/" + match_type + "/group_name/" + "fake_group"
logger.info("Expecting error.")
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 1, 1)
check_json_value(obj, "error", str, None)

#servers
url_path_match = "servers"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj[0], 1, 200)
check_json_value(obj[0], "server_name", str, None)

url_path_match = "servers/detail"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj[0], 1, 200)
check_json_value(obj[0]["data"], "g_gametype", str, 1)

url_path_match = "servers/region/" + region
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 200)
check_json_value(obj[0], "server_name", str, None)

url_path_match = "servers/region/" + region + "/active"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 200)
check_json_value(obj[0], "server_name", str, None)

category = "elo"
type_ = "6"
url_path_match = "leaders/" + category + "/region/" + region + "/type/" + type_
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 20)
check_json_value(obj[0], "guid", str, 32)

category = "kdr"
type_ = "6"
url_path_match = "leaders/" + category + "/region/" + region + "/type/" + type_
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 20)
check_json_value(obj[0], "guid", str, 32)

category = "acc"
type_ = "3"
url_path_match = "leaders/" + category + "/region/" + region + "/type/" + type_
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 20)
check_json_value(obj[0], "guid", str, 32)

category = "elo"
type_ = "6"
url_path_match = "leaders/" + category + "/region/" + region + "/type/" + type_ + "/limit/" + "100"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 100)
check_json_value(obj[0], "guid", str, 32)


url_path_match = "aliases/player/" + player_guid
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 100)
check_json_value(obj[0], "guid", str, 32)

url_path_match = "aliases/search/" + "unnam"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 100)
check_json_value(obj[0], "guid", str, 32)

type_ = "6"
url_path_match = f"eloprogress/player/{player_guid}/region/{region}/type/{type_}"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 100)
check_json_value(obj[0], "value", int, None)

match_id_local = "1632108123"
url_path_match = f"eloprogress/match/{match_id_local}"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 100)
check_json_value(obj[0], "value", int, None)

url_path_match = "aliases/recent/limit/5"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 5)
check_json_value(obj[0], "guid", str, 32)



