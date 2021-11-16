import boto3
import logging
import json
import os
import urllib3
from boto3.dynamodb.conditions import Key

# for local testing use actual table
# for lambda execution runtime use dynamic reference
if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('discordhook')
logger.setLevel(log_level)


def handler(event, context):
    """Discord notification for a match end."""
    round_id = 2
    if isinstance(event, int):
        match_id = str(event)
        round_id = "2"
    elif isinstance(event, dict):
        match_id = event["matchid"]
        round_id = str(event["roundid"])
    else:
        logger.warning("Received unexpected input for match_id " + str(event))
        return {"error": "Exiting function due to bad input."}

    logger.info("Processing match id " + match_id)

    notify_discord(match_id, round_id)

    logger.info("Done.")


def notify_discord(match_id, round_id):
    """Process logic."""
    try:
        response_match = ddb_table.get_item(Key={'pk': "match", 'sk': match_id + round_id})

        if round_id == "2":
            players = get_elo_progress_info(match_id)

        logger.info("Got match info for " + match_id + round_id)
        data_str = response_match["Item"]["data"]
        data_json = json.loads(data_str)
        server_name = data_json["server_name"]
        map_ = data_json["map"]

        match_type_tokens = response_match["Item"].get('lsipk', "##").split("#")
        match_type = "#".join(match_type_tokens[0:2])

        response_server = ddb_table.get_item(Key={'pk': "server", 'sk': server_name})
        logger.info("Got server info for " + server_name)
        server_data_json = response_server["Item"]["data"]
        ip = server_data_json.get("serverIP", None)
        ip_str = ""
        if ip:
            ip_str = ip

        time = round((int(data_json["round_end"]) - int(data_json["match_id"])) / 60, 1)

        logger.info("Looking for hook info on " + match_type + "#" + server_name)
        response_hook = ddb_table.get_item(Key={'pk': "discordhook", 'sk': match_type + "#" + server_name})
        if "Item" in response_hook:
            logger.info("Got hook info on " + match_type + "#" + server_name)
        else:
            logger.info("Got NO hook info on " + match_type + "#" + server_name)
        hook_url = response_hook.get("Item", {}).get("discordhook", None)
        hook_active = response_hook.get("Item", {}).get("hookactive", "no")
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        logger.error("Failed to send match notification to discord + \n" + error_msg)
        raise

    if hook_active == "yes" and hook_url:
        if round_id == "1":
            payload = build_round1_payload(match_id, server_name, ip_str, map_, time)
            send_notification(hook_url, payload)
        elif round_id == "2":
            payload = build_round2_payload(match_id, server_name, ip_str, map_, time, players)
            send_notification(hook_url, payload)
        else:
            logger.error("What round is it??")
    else:
        logger.info("Hook is not active or not found.")


def get_elo_progress_info(match_id):
    """Get elo progress for the match."""
    projections = "#data_value, gsi1sk, elo, performance_score, real_name"
    expressionAttributeNames = {'#data_value': 'data'}
    response_eloprogress = ddb_table.query(IndexName="gsi1", KeyConditionExpression=Key("gsi1pk").eq("eloprogressmatch") & Key("gsi1sk").begins_with(match_id), ProjectionExpression=projections, ExpressionAttributeNames=expressionAttributeNames)

    players = []
    for item in response_eloprogress["Items"]:
        player_info = [0, item['real_name'], int(item['data']), int(item['elo']), int(item['performance_score'])]
        players.append(player_info)

    players.sort(key=lambda x: x[3], reverse=True)
    return players


def send_notification(hook_url, payload):
    """Post payload to discord hook url."""
    if __name__ == "__main__":
        hook_url = ""
        # print("DO NOT CHECK IN\n" * 5)

    http = urllib3.PoolManager()
    response = http.request('POST', hook_url,
                            headers={'Content-Type': 'application/json'},
                            body=json.dumps(payload).encode('utf-8'))

    if response.status == 204:
        logger.info("Discord hook call successfull")
    else:
        logger.info("Discord hook call abnormal return code " + str(response.status))
        logger.info(response._body)


def build_round1_payload(match_id, server_name, ip_str, map_, time):
    """Create a message about round1 over."""
    payload = {
        "embeds": [
            {
                "author": {
                    "name": "RTCWProAPI",
                    "icon_url": "https://rtcwpro.com/images/RtCWProVector.png"
                },
                "description": f"**{server_name}** posted round 1 time of about {time} minutes.",
                "color": 15258703,
                "fields": [
                    {
                        "name": "Map",
                        "value": map_,
                        "inline": True
                    },
                    {
                        "name": "IP",
                        "value": ip_str,
                        "inline": True
                    }
                ]
            }]
    }
    return payload


def build_round2_payload(match_id, server_name, ip_str, map_, time, players):
    """Build payload."""
    colwith_1 = 5
    colwith_2 = 20
    colwith_3 = 6
    colwith_4 = 6
    # colwith_5 = 20
    headers = ""
    headers += "№".ljust(colwith_1)
    headers += "Nickname".ljust(colwith_2)
    headers += "+/-".ljust(colwith_3)
    headers += "ELO".ljust(colwith_4)
    # headers += "Performance score".ljust(colwith_5)
    content = headers + "\n"
    for index, player in enumerate(players):
        row = ""
        row += str(index + 1).ljust(colwith_1)
        row += str(player[1]).ljust(colwith_2)
        if player[2] > 0:
            elo_delta = "+" + str(player[2])
        else:
            elo_delta = str(player[2])
        row += str(elo_delta).ljust(colwith_3)
        row += str(player[3]).ljust(colwith_4)
        # row += str(player[4]).ljust(colwith_5)
        content += row + "\n"

    payload = {
        "embeds": [
            {
                "author": {
                    "name": "RTCWProAPI",
                    "icon_url": "https://rtcwpro.com/images/RtCWProVector.png"
                },
                "description": f"**{server_name}** posted match result after {time} minutes. [https://stats.rtcwpro.com/matches/{match_id}](https://stats.rtcwpro.com/matches/{match_id}) `\n" + content + "`",
                "color": 15258703,
                "fields": [
                    {
                        "name": "Map",
                        "value": map_,
                        "inline": True
                    },
                    {
                        "name": "IP",
                        "value": ip_str,
                        "inline": True
                    }
                ]
            }]
    }
    return payload


if __name__ == "__main__":
    event = 1634610760
    event = {"matchid": "1634610760", "roundid": 1}
    handler(event, None)
