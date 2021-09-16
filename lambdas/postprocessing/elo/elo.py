import boto3
import logging
import os
from elo_calc import process_rtcwpro_elo

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
logger = logging.getLogger('elo')
logger.setLevel(log_level)


def handler(event, context):
    """Calculate elo changes for this match."""
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    if not isinstance(event, int):
        logger.warning("Received unexpected input for match_id " + str(event))
        return {"error": "Exiting function due to bad input."}
    else:
        match_id = str(event)

    logger.info("Processing match id " + match_id)
    
    process_rtcwpro_elo(ddb_table, ddb_client, match_id, log_stream_name)

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': 'Elo function finished without exceptions.'
    }


if __name__ == "__main__":
    event = 1623640685
    handler(event, None)
