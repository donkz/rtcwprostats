import boto3
import logging
import os
from group_cache_calc import process_rtcwpro_summary

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
logger = logging.getLogger('group_cacher')
logger.setLevel(log_level)

def handler(event, context):
    """Merge summaries for a player including match."""
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    group_name = event["taskdetail"]
    logger.info("Processing match group: " + group_name)
    process_rtcwpro_summary(ddb_table, ddb_client, group_name, log_stream_name)
    return {"result": "ok"}

if __name__ == "__main__":
    event = {
      "tasktype": "group_cacher",
      "taskdetail": "gather-17463-1635535421"
    }
    handler(event, None)
