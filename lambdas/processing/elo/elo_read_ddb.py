import boto3
import logging
import json

ddb = boto3.client('dynamodb')
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # set to DEBUG for verbose boto output
logging.basicConfig(level=logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('rtcwprostats')

matchid = "16096270141"
try:
    response = table.get_item(Key={"pk": "match", 'sk': matchid})  # , ReturnConsumedCapacity='TOTAL')
except ddb.exceptions.ClientError as e:
    print(e.response['Error']['Message'])
else:
    if "Item" in response:
        stats = json.loads(response['Item']["data"])

        for team in stats:
            for playerguid, stat in team.items():
                print(playerguid)
