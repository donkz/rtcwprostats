import requests
import json
import logging


log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('gamelog')
logger.setLevel(log_level)

url_api = 'https://rtcwproapi.donkanator.com/'
api_path = "gamelogs/"
# match_id = "1639976879" 
match_id = "1639975736" 
# match_id = "1639975094" 
# match_id = "1639376151"  # get more from https://rtcwproapi.donkanator.com/matches/type/na/6. Use match_round_id value
round_id = "1"
url = url_api + api_path + match_id + round_id

logger.info("Reading gamelog from match {match_id} round {round_id}".format(match_id=match_id, round_id=round_id))
response = requests.get(url)
gamelog_r1 = json.loads(response.text)

round_id = "2"
url = url_api + api_path + match_id + round_id

logger.info("Reading gamelog from match {match_id} round {round_id}".format(match_id=match_id, round_id=round_id))
response = requests.get(url)
gamelog_r2 = json.loads(response.text)

gamelog = []
gamelog.extend(gamelog_r1)
gamelog.extend(gamelog_r2)
# there could be more than 2 or just 1 rounds here


# everything above this line is just a test setup
# you now have 2 rounds of one match
# it is a list ordered in the same way RTCWPro produced it
# -----------------------------------------------
# below is your code
# please create your own class that once instantiated takes events line by line and
# produces a dictionary in the form of {"award_name": {"guid":"1234", "value":"15"}}
# please use native python and packages that come by default
# for full list refer here: https://insidelambda.com/lambda-python38.txt


from longest_kill import LongestKill
from frontliner import Frontliner
from megakill import MegaKill
from top_feuds import TopFeuds
# add your class here

longest_kill = LongestKill()
frontliner = Frontliner()
megakill = MegaKill()
top_feuds = TopFeuds()
# init your class here

# Loop through all events in the match or a group and feed events to different award calculator classes
for rtcw_event in gamelog:
    longest_kill.process_event(rtcw_event)
    frontliner.process_event(rtcw_event)
    megakill.process_event(rtcw_event)
    top_feuds.process_event(rtcw_event)
    # your_class.process_event(rtcw_event)

awards = {}
awards.update(longest_kill.get_all_top_results())
awards.update(frontliner.get_all_top_results())
awards.update(megakill.get_all_top_results())
# insert your result here

print(top_feuds.get_custom_results())

print(awards)
