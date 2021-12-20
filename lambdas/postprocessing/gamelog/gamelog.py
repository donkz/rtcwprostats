import boto3
import logging
import os
import json
from gamelog_process.gamelog_calc import process_gamelog

# for local testing use actual table
# for lambda execution runtime use dynamic reference
if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('gamelog_main')
logger.setLevel(log_level)


def handler(event, context):
    """Calculate elo changes for this match."""
    print("-----------EVENT-------------\n")
    print(json.dumps(event))
    
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    match_or_group_id = event

    logger.info("Processing match/group id: " + str(match_or_group_id))
    
    try:
        process_gamelog(ddb_table, ddb_client, match_or_group_id, log_stream_name)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to process gamelog for " + str(match_or_group_id) + ".\n" + error_msg
        logger.error(message)
    else:
        message = "Award processing finished for " + str(match_or_group_id)
        logger.info(message)
        
    return {"Final Message" : message}

if __name__ == "__main__":
    event = 1639371426
    event = "gather-Wednesday-1637863062"
    handler(event, None)