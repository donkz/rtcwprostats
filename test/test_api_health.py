import requests
import os
import time
import json
import sys
import boto3
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
test_guids = ['2918F80471E175']
seach_player = "donk"

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
check_num_elements(obj, 2, 40)
check_json_value(obj[0], "match_id", str, 10)

# /matches/recent/number
url_path_match = "matches/recent/92"
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 10, 40)
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

# /wstats/{match_id}
url_path_match = "wstats/" +  match_id
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 2, 2)
check_json_value(obj, "match_id", str, 10)
check_json_value(obj, "wstatsall", list, None)

# /wstats/player/{player_guid}/match/{match_id}
url_path_match = "wstats/player/" + player_guid + "/match/" + match_id
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, dict)
check_num_elements(obj, 2, 3)
check_json_value(obj, "match_id", str, 10)
check_json_value(obj, "wstats", list, None)
check_json_value(obj, "player_guid", str, 14)

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
check_json_value(obj, "player_guid", str, 14)

# /player/search/{begins_with}
url_path_match = "player/search/" +  seach_player
obj = get_api_response_as_json(url_path_match)
check_obj_type(obj, list)
check_num_elements(obj, 1, 30)
check_json_value(obj[0], "real_name", str, 5)