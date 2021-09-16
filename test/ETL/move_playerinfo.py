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
logger = logging.getLogger('move_playerinfo')
logger.setLevel(log_level)

log_stream_name = "local"

def move_elos(ddb_table):
     pk = "player"
     skname = "sk"
     begins_with = "playerinfo#"
     response = ddb_table.query(KeyConditionExpression=Key('pk').eq(pk) & Key(skname).begins_with(begins_with),
#                                FilterExpression=Attr('elo_migration_phase').not_exists(),
                                ProjectionExpression="sk, elos, real_name")
     elo_items = []
     if response['Count'] > 0:
        for record in response["Items"]:
            if record["sk"] in ['playerinfo#', 'playerinfo#1', 'playerinfo#0']:
                continue
            guid = record["sk"].replace("playerinfo#","")
            if len(guid) == len("206A67CC89B45B"):
                continue
            real_name = record.get("real_name", guid)
            elos = record.get("elos", None)
            if elos:
                for elokey in elos:
                    if len(elokey.split("#")) < 3:
                        logger.error(elokey)
                    if ("#elo" in elokey 
                        and "unk" not in elokey 
                        and "notype" not in elokey
                        and "6plus" not in elokey):
                        matchtype = elokey.replace("#elo","")
                        elo = elos[elokey]
                        games = elos[matchtype + "#games"]
                        ts = datetime.now().isoformat()
                elo_item = ddb_prepare_elo_item(guid, matchtype, elo, games, ts, real_name)
                elo_items.append(elo_item)
        logger.info("Copying names")
        ddb_batch_write(ddb_client, ddb_table.name, elo_items)

def ddb_prepare_elo_item(guid, matchtype, elo, games, ts, real_name):    
    elo_item = {
            'pk'            : "player"+ "#" + guid,
            'sk'            : "elo#" + matchtype,
            # 'lsipk'         : "",
            'gsi1pk'        : "leaderelo#" + matchtype,
            'gsi1sk'        : str(elo).zfill(3),
            'data'          : str(elo),
            'games'         : games,
            'updated'       : ts,
            "real_name"     : real_name
        }
    return elo_item

def move_real_names(ddb_table):
    pk = "player"
    skname = "sk"
    begins_with = "playerinfo#"
    response = ddb_table.query(KeyConditionExpression=Key('pk').eq(pk) & Key(skname).begins_with(begins_with),
                               FilterExpression=Attr('real_name').exists(),
                               ProjectionExpression="sk, real_name")
    
    real_name_items = []
    if response['Count'] > 0:
        for record in response["Items"]:
            guid = record["sk"].replace("playerinfo#","")
            real_name = record["real_name"]
            ts = datetime.now().isoformat()
            realname_item = ddb_prepare_realname_item(guid, real_name, ts)
            real_name_items.append(realname_item)
    
    logger.info("Copying names")
    ddb_batch_write(ddb_client, ddb_table.name, real_name_items)

def ddb_prepare_realname_item(guid, real_name, ts):    
    realname_item = {
            'pk'            : "player"+ "#" + guid,
            'sk'            : "realname",
            # 'lsipk'         : "realname#" + real_name,
            'gsi1pk'        : "realname",
            'gsi1sk'        : "realname#" + real_name,
            'data'          : real_name
        }
    return realname_item
    
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
    move_real_names(ddb_table)
    # move_elos(ddb_table)




