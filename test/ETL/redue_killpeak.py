import boto3
import logging
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from botocore.exceptions import ClientError
import time as _time

# for local testing use actual table
# for lambda execution runtime use dynamic reference
TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"


dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(log_level)

log_stream_name = "local"

def move_killpeak_region_type(ddb_table, region, type_):
    pk_base = "leaderkillpeak#"
    pk = pk_base + region + "#" + type_
    response = ddb_table.query(IndexName='gsi1', KeyConditionExpression=Key('gsi1pk').eq(pk))
    
    new_kp_items = []
    if response['Count'] > 0:
       for record in response["Items"]:
           new_record = record.copy()
           new_record['sk'] = record["sk"].replace("killpeak#", "achievement#Killpeak#")
           new_record['gsi1pk'] = record["gsi1pk"].replace("leaderkillpeak#", "leader#Killpeak#")
           del new_record['games']
           new_kp_items.append(new_record)
       logger.info("Copying names")
       ddb_batch_write(ddb_client, ddb_table.name, new_kp_items)
    
def create_batch_write_structure(table_name, items, start_num, batch_size):
    """
    Create item structure for passing to batch_write_item
    :param table_name: DynamoDB table name
    :param items: large collection of items
    :param start_num: Start index
    :param num_items: Number of items
    :return: dictionary of tables to write to
    """
    
    serializer = boto3.dynamodb.types.TypeSerializer()
    item_batch = { table_name: []}
    item_batch_list = items[start_num : start_num + batch_size]
    if len(item_batch_list) < 1:
        return None
    for item in item_batch_list:
        item_serialized = {k: serializer.serialize(v) for k,v in item.items()}
        item_batch[table_name].append({'PutRequest': {'Item': item_serialized}})
                
    return item_batch

def ddb_batch_write(client, table_name, items):
        message = ""
        num_items = len(items)
        logger.info(f'Performing ddb_batch_write to dynamo with {num_items} items.')
        start = 0
        batch_size = 25
        while True:
            # Loop adding 25 items to dynamo at a time
            request_items = create_batch_write_structure(table_name,items, start, batch_size)
            if not request_items:
                break
            try: 
                response = client.batch_write_item(RequestItems=request_items)
            except ClientError as err:
                logger.error(err.response['Error']['Message'])
                logger.error("Failed to run full batch_write_item")
                raise
            if len(response['UnprocessedItems']) == 0:
                logger.info(f'Wrote a batch of about {batch_size} items to dynamo')
            else:
                # Hit the provisioned write limit
                logger.warning('Hit write limit, backing off then retrying')
                sleep_time = 5 #seconds
                logger.warning(f"Sleeping for {sleep_time} seconds")
                _time.sleep(sleep_time)

                # Items left over that haven't been inserted
                unprocessed_items = response['UnprocessedItems']
                logger.warning('Resubmitting items')
                # Loop until unprocessed items are written
                while len(unprocessed_items) > 0:
                    response = client.batch_write_item(RequestItems=unprocessed_items)
                    # If any items are still left over, add them to the
                    # list to be written
                    unprocessed_items = response['UnprocessedItems']

                    # If there are items left over, we could do with
                    # sleeping some more
                    if len(unprocessed_items) > 0:
                        sleep_time = 5 #seconds
                        logger.warning(f"Sleeping for {sleep_time} seconds")
                        _time.sleep(sleep_time)

                # Inserted all the unprocessed items, exit loop
                logger.warning('Unprocessed items successfully inserted')
                break
            if response["ResponseMetadata"]['HTTPStatusCode'] != 200:
                message += f"\nBatch {start} returned non 200 code"
                logger.warning(message)
            start += 25

if __name__ == "__main__":
    regions = ["na","eu","sa", "unk"]
    types = ["6","3", "6plus", "unk"]
    move_killpeak_region_type(ddb_table, regions[2], types[1])
    # move_elos(ddb_table)




