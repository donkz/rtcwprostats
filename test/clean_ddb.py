import boto3

dynamo = boto3.resource('dynamodb')
tableName = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
table = dynamo.Table(tableName)

def truncateTable(table):
    
    #get the table keys
    tableKeyNames = [key.get("AttributeName") for key in table.key_schema]
    
    """
    NOTE: there are reserved attributes for key names, please see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
    if a hash or range key is in the reserved word list, you will need to use the ExpressionAttributeNames parameter
    described at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan
    """

    #Only retrieve the keys for each item in the table (minimize data transfer)
    ProjectionExpression = ", ".join(tableKeyNames)
    
    response = table.scan(ProjectionExpression=ProjectionExpression)
    data = response.get('Items')
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression=ProjectionExpression, 
            ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    with table.batch_writer() as batch:
        for each in data:
            batch.delete_item(
                Key={key: each[key] for key in tableKeyNames}
            )
            
truncateTable(table)

