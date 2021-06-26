import requests
import os
import base64
import time
import json
import sys
import boto3
import json
import logging

def get_files(test_dir):
    test_files = [] 
    for subdir, dirs, files in os.walk(test_dir):
            for file in files:
                #print os.path.join(subdir, file)
                filepath = subdir + os.sep + file
                if filepath.endswith(".json"):
                    test_files.append(filepath)
    return test_files

def submit_file(file_name):
    logger.debug("Reading from " + os.path.abspath(file_name))
    #logger.info("Test files left " + str(len(test_files)))
    with open(file_name) as file:
        content = file.read()
    
    match = json.loads(content)
    match_id_round ="restarted"
    if '"label": "map_restart"' not in content:
        match_id_round = match["gameinfo"]["match_id"] + match["gameinfo"]["round"]
        
        content_string_bytes = content.encode("ascii") 
        base64_bytes = base64.b64encode(content_string_bytes) 
        base64_string = base64_bytes.decode("ascii") 
        str_len = str(int(len(base64_string)/1024))
        logger.debug(f"Submitting string of {str_len} KB")
        
        
        # =============================================================================
        # TEST SUBMISSION OF STATS TO WEB API
        # =============================================================================
        url = url_api+url_path_submit
        response = requests.post(url, data=base64_string, headers={'matchid': match_id_round[0:-1], 'x-api-key':'rtcwproapikeythatisjustforbasicauthorization'})
        logger.info(response.text)
    else:
        logger.debug("map_restart detected, not submitting")
    return match_id_round

boto3.set_stream_logger('boto3.resources', logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE")

url_api = 'https://rtcwproapi.donkanator.com/'
url_path_submit = 'submit'
test_dir = r".\gamestats5"
test_dir = r"C:\c\.wolf\rtcwpro\stats\\"

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("test_submit")
logger.setLevel(log_level)


test_files = get_files(test_dir)

file_name = test_files[0]
test_files.remove(file_name)
match_id_round = submit_file(file_name)

#curl --location --request POST 'https://rtcwproapi.donkanator.com/submit' 
#--header 'matchid: 123455678' 
#--header 'x-api-key: rtcwproapikeythatisjustforbasicauthorization' 
#--data-raw 'ewogICAgImdsb3NzYXJ5IjogewogICAgICAgICJ0aXRsZSI6ICJleGFtcGxlIGdsb3NzYXJ5IiwKCQkiR2xvc3NEaXYiOiB7CiAgICAgICAgICAgICJ0aXRsZSI6ICJTIiwKCQkJIkdsb3NzTGlzdCI6IHsKICAgICAgICAgICAgICAgICJHbG9zc0VudHJ5IjogewogICAgICAgICAgICAgICAgICAgICJJRCI6ICJTR01MIiwKCQkJCQkiU29ydEFzIjogIlNHTUwiLAoJCQkJCSJHbG9zc1Rlcm0iOiAiU3RhbmRhcmQgR2VuZXJhbGl6ZWQgTWFya3VwIExhbmd1YWdlIiwKCQkJCQkiQWNyb255bSI6ICJTR01MIiwKCQkJCQkiQWJicmV2IjogIklTTyA4ODc5OjE5ODYiLAoJCQkJCSJHbG9zc0RlZiI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgInBhcmEiOiAiQSBtZXRhLW1hcmt1cCBsYW5ndWFnZSwgdXNlZCB0byBjcmVhdGUgbWFya3VwIGxhbmd1YWdlcyBzdWNoIGFzIERvY0Jvb2suIiwKCQkJCQkJIkdsb3NzU2VlQWxzbyI6IFsiR01MIiwgIlhNTCJdCiAgICAgICAgICAgICAgICAgICAgfSwKCQkJCQkiR2xvc3NTZWUiOiAibWFya3VwIgogICAgICAgICAgICAgICAgfQogICAgICAgICAgICB9CiAgICAgICAgfQogICAgfQp9'

sleep_time = 10 #seconds
logger.debug(f"Sleeping for {sleep_time} seconds")
time.sleep(sleep_time)

# =============================================================================
# CONFIRM DATA EXISTS IN THE DATABASE
# =============================================================================


# ===========
# match record
# ===========
response = table.get_item(Key={"pk": "match", 'sk': match_id_round}, ReturnConsumedCapacity='TOTAL')
if "Item" not in response:
    logger.warning("Item not found.")
else:
    print("Cost of the call: " + str(response['ConsumedCapacity']['CapacityUnits']) + " capacity units")
    data = json.loads(response['Item']["data"])
    match_id = data['match_id']
    round_id = data['round']
    map_name = data['map']
    logger.info(f"Found match {match_id} round {round_id} on {map_name}")

# ========
# statsall
# ========
response = table.get_item(Key={"pk": "statsall", 'sk': match_id_round[0:-1]}, ReturnConsumedCapacity='TOTAL')
if "Item" not in response:
    logger.warning("Item statsall not found.")
else:
    print("Cost of the call: " + str(response['ConsumedCapacity']['CapacityUnits']) + " capacity units")
    data = json.loads(response['Item']["data"])
    stats_match_id = match_id_round[0:-1]
    num_players = len(data[0].keys()) + len(data[1].keys())
    logger.info(f"Found statsall for {stats_match_id} with {num_players} players")
    
# ========
#wstatsall
# ========
response = table.get_item(Key={"pk": "wstatsall", 'sk': match_id_round[0:-1]}, ReturnConsumedCapacity='TOTAL')
if "Item" not in response:
    logger.warning("Item wstatsall not found.")
else:
    print("Cost of the call: " + str(response['ConsumedCapacity']['CapacityUnits']) + " capacity units")
    data = json.loads(response['Item']["data"])
    wstats_match_id = match_id_round[0:-1]
    weapons = 0
    num_players = len(data)
    for player in data:
        weapons += len(player[list(player.keys())[0]])
    logger.info(f"Found wstatsall for match {wstats_match_id} with {num_players} and {weapons} weapons")

    
# =============================================================================
# CONFIRM DATA IS AVAILABLE VIA WEB API
# =============================================================================
url_path_match = "stats/" +  stats_match_id
url = url_api + url_path_match

response = requests.get(url)
statsall = json.loads(response.text)
num_players = len(statsall["statsall"][0].keys()) + len(statsall["statsall"][1].keys())
logger.info(f"Found web statsall for match {stats_match_id} with {num_players}")

os.chdir("C:\\Users\\xxx\\Documents\\Github\\rtcwprostats\\test")
def submit_batch(test_files, size):
    submitted = 0
    
    try:
        with open('submitted_files.txt','r') as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        print("File not found, starting fresh.")
        lines = []
    submitted_files_dict = dict(zip(lines,"1"*len(lines)))
    submitted_files = open("submitted_files.txt", "a")

    try: 
        print("Scanning through " + str(len(test_files)) + " files.")
        for file_name in test_files:
            # print("looping through " + file_name)
            if file_name.split('\\')[-1] in submitted_files_dict:
                # print("Skipping submitted file.")
                #test_files.remove(file_name)
                continue
            try:
                submit_file(file_name)
                submitted +=1
                logger.info("Submitted " + str(submitted) + " files out of " + str(size))
                submitted_files.write(file_name.split('\\')[-1]+"\n")
                time.sleep(1)
            except ConnectionError as ex:
                print(ex.args)
                raise
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                error_msg = template.format(type(ex).__name__, ex.args)
                print(error_msg)
            
            if submitted >= size:
                print("Finished " +str(submitted) + "//" + str(size) + " iterations.")
                break
            
        print("Out of cycle.")
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to load file " + file_name + "\n" + error_msg
        logger.info(message)
    finally:
        submitted_files.close()

    
    
    
    
        
    