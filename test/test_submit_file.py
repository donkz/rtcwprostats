import requests
import os
import base64
import time
import json
import sys


url_api = 'https://rtcwproapi.donkanator.com/'
url_path_submit = 'submit'

ran_today = True
try:
    len(test_files)
    print("Running subsequently")
except NameError:
    print("Running for the first time")
    ran_today = False
    
sys.exit()

if not ran_today: 
    test_files = [] 
    for subdir, dirs, files in os.walk(r".\gamestats5"):
            for file in files:
                #print os.path.join(subdir, file)
                filepath = subdir + os.sep + file
                if filepath.endswith(".json"):
                    test_files.append(filepath)
                
                
#file_name = r".\gamestats5\gameStats_match_1610684722_round_2_mp_base.json"
file_name = test_files[0]
test_files.remove(file_name)

print("Reading from " + os.path.abspath(file_name))
print("Test files left " + str(len(test_files)))
with open(file_name) as file:
    content = file.read()

match = json.loads(content)
match_id_round = match["gameinfo"]["match_id"] + match["gameinfo"]["round"]

content_string_bytes = content.encode("ascii") 
base64_bytes = base64.b64encode(content_string_bytes) 
base64_string = base64_bytes.decode("ascii") 
str_len = str(int(len(base64_string)/1024))
print(f"Submitting string of {str_len} KB")


# =============================================================================
# TEST SUBMISSION OF STATS TO WEB API
# =============================================================================
url = url_api+url_path_submit
response = requests.post(url, data=base64_string, headers={'matchid': match_id_round[0:-1], 'x-api-key':'rtcwproapikeythatisjustforbasicauthorization'})
print(response.text)

#curl --location --request POST 'https://rtcwproapi.donkanator.com/submit' 
#--header 'matchid: 123455678' 
#--header 'x-api-key: rtcwproapikeythatisjustforbasicauthorization' 
#--data-raw 'ewogICAgImdsb3NzYXJ5IjogewogICAgICAgICJ0aXRsZSI6ICJleGFtcGxlIGdsb3NzYXJ5IiwKCQkiR2xvc3NEaXYiOiB7CiAgICAgICAgICAgICJ0aXRsZSI6ICJTIiwKCQkJIkdsb3NzTGlzdCI6IHsKICAgICAgICAgICAgICAgICJHbG9zc0VudHJ5IjogewogICAgICAgICAgICAgICAgICAgICJJRCI6ICJTR01MIiwKCQkJCQkiU29ydEFzIjogIlNHTUwiLAoJCQkJCSJHbG9zc1Rlcm0iOiAiU3RhbmRhcmQgR2VuZXJhbGl6ZWQgTWFya3VwIExhbmd1YWdlIiwKCQkJCQkiQWNyb255bSI6ICJTR01MIiwKCQkJCQkiQWJicmV2IjogIklTTyA4ODc5OjE5ODYiLAoJCQkJCSJHbG9zc0RlZiI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgInBhcmEiOiAiQSBtZXRhLW1hcmt1cCBsYW5ndWFnZSwgdXNlZCB0byBjcmVhdGUgbWFya3VwIGxhbmd1YWdlcyBzdWNoIGFzIERvY0Jvb2suIiwKCQkJCQkJIkdsb3NzU2VlQWxzbyI6IFsiR01MIiwgIlhNTCJdCiAgICAgICAgICAgICAgICAgICAgfSwKCQkJCQkiR2xvc3NTZWUiOiAibWFya3VwIgogICAgICAgICAgICAgICAgfQogICAgICAgICAgICB9CiAgICAgICAgfQogICAgfQp9'

sleep_time = 10 #seconds
print(f"Sleeping for {sleep_time} seconds")
time.sleep(sleep_time)

# =============================================================================
# CONFIRM DATA EXISTS IN THE DATABASE
# =============================================================================
import boto3
import json

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('rtcwprostats')

# ===========
# match record
# ===========
response = table.get_item(Key={"pk": "match", 'sk': match_id_round}, ReturnConsumedCapacity='TOTAL')
if "Item" not in response:
    print("Item not found.")
else:
    print("Cost of the call: " + str(response['ConsumedCapacity']['CapacityUnits']) + " capacity units")
    data = json.loads(response['Item']["data"])
    match_id = data['match_id']
    round_id = data['round']
    map_name = data['map']
    print(f"Found match {match_id} round {round_id} on {map_name}")

# ========
# statsall
# ========
response = table.get_item(Key={"pk": "statsall", 'sk': match_id_round[0:-1]}, ReturnConsumedCapacity='TOTAL')
if "Item" not in response:
    print("Item statsall not found.")
else:
    print("Cost of the call: " + str(response['ConsumedCapacity']['CapacityUnits']) + " capacity units")
    data = json.loads(response['Item']["data"])
    stats_match_id = match_id_round[0:-1]
    num_players = len(data[0].keys()) + len(data[1].keys())
    print(f"Found statsall for {stats_match_id} with {num_players} players")
    
# ========
#wstatsall
# ========
response = table.get_item(Key={"pk": "wstatsall", 'sk': match_id_round[0:-1]}, ReturnConsumedCapacity='TOTAL')
if "Item" not in response:
    print("Item wstatsall not found.")
else:
    print("Cost of the call: " + str(response['ConsumedCapacity']['CapacityUnits']) + " capacity units")
    data = json.loads(response['Item']["data"])
    wstats_match_id = match_id_round[0:-1]
    weapons = 0
    num_players = len(data)
    for player in data:
        weapons += len(player[list(player.keys())[0]])
    print(f"Found wstatsall for match {wstats_match_id} with {num_players} and {weapons} weapons")

    
# =============================================================================
# CONFIRM DATA IS AVAILABLE VIA WEB API
# =============================================================================
url_path_match = "stats/" +  stats_match_id
url = url_api + url_path_match

response = requests.get(url)
statsall = json.loads(response.text)
num_players = len(statsall["statsall"][0].keys()) + len(statsall["statsall"][1].keys())
print(f"Found web statsall for match {stats_match_id} with {num_players}")