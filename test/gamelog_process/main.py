import requests
import json
import logging


log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('gamelog')
logger.setLevel(log_level)

url_api = 'https://rtcwproapi.donkanator.com/'
api_path = "gamelogs/"
match_round_id = "16309530601" #get more from https://rtcwproapi.donkanator.com/matches/type/na/6. Use match_round_id value
url = url_api + api_path + match_round_id

logger.info("Reading gamelog from match {match} round {round_id}".format(match = match_round_id[0:-1], round_id = match_round_id[-1:]))
response = requests.get(url)
gamelog = json.loads(response.text)

# everything above this line is just a test setup
# -----------------------------------------------
# below is your code
# please put the definitions into its own module
# and expose just the main function to get a dictionary in the form of {"award_name": {results}}
# please use native python and packages that come by default
# for full list refer here: https://insidelambda.com/lambda-python38.txt


from mymodule import my_function
my_result_dict = my_function(gamelog)
print(my_result_dict)




