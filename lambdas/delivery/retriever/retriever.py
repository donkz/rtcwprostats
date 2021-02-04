import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import time

TABLE_NAME = "rtcwprostats"
#dynamodb_client = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)


def handler(event, context):
    result = "[x] Request failed."

    if __name__ == "__main__":
        log_stream_name  = "local"
    else:
        log_stream_name = context.log_stream_name
    log_stream = "Lambda log stream name:" + log_stream_name
    

    #print('request: {}'.format(json.dumps(event)))
    api_path = event["resource"]
    #print(api_path)
    
    if api_path == "/matches/{proxy+}":
        print("Processing " + api_path)
        if "proxy" in event["pathParameters"]:
            match_id = event["pathParameters"]["proxy"]
            pk = "match"
            sk = match_id
            response = get_item(pk, sk, ddb_table, log_stream_name)
    
    
    if api_path == "/stats/player/{player_guid}":
        print("Processing " + api_path)
        if "player_guid" in event["pathParameters"]:
            guid = event["pathParameters"]["player_guid"]
            pk = "stats" + "#" + guid
            skhigh = int(time.time())
            sklow = skhigh - 60*60*24*30
            responses = get_range(pk, str(sklow), str(skhigh), ddb_table, log_stream_name)
            
            if "error" not in responses:
                #logic specific to /stats/player/{player_guid}            
                data = []
                for line in responses:
                    data_line = json.loads(line["data"])
                    data_line["match_id"] = line["sk"] 
                    data_line["type"] = line["gsi1pk"].replace("stats#","")
                    data.append(data_line)
            else:
                data = responses
        
        
    if api_path == "/stats/{match_id}":
        print("Processing " + api_path)
        if "match_id" in event["pathParameters"]:
            match_id = event["pathParameters"]["match_id"]
            pk = "statsall"
            sk = match_id
            response = get_item(pk, sk, ddb_table, log_stream_name)
            
            #logic specific to /stats/{match_id}
            if "error" not in response:
                data = { "statsall" : json.loads(response["data"])}
                data["match_id"] = response["sk"]
                data["type"] = response["gsi1pk"].replace("stats#","")
            else:
                data = response
            
    return  {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': json.dumps(data)
    }

def get_item(pk, sk, table, log_stream_name):
    item_info = pk + ":" + sk + ". Logstream: " + log_stream_name
    try:
        response = table.get_item(Key={'pk': pk, 'sk': sk})
    except ClientError as e:
        print("Exception occurred: " + e.response['Error']['Message'])
        result =  make_error_dict("[x] Client error calling database: ",item_info)
    else:
        if "Item" in response:    
            result = response['Item']
        else:
            result =  make_error_dict("[x] Item does not exist: ", item_info)
    return result

def get_range(pk, sklow, skhigh, table, log_stream_name):
    item_info = pk + ":" + sklow + " to " + skhigh + ". Logstream: " + log_stream_name
    try:
        response = table.query(KeyConditionExpression=Key('pk').eq(pk) & Key('sk').between(sklow,skhigh))
    except ClientError as e:
        print("Exception occurred: " + e.response['Error']['Message'])
        result =  make_error_dict("[x] Client error calling database: ",item_info)
    else:
        if response['Count'] > 0:    
            result = response['Items']
        else:
            result =  make_error_dict("[x] Items do not exist: ",item_info)
    return result

def make_error_dict(message, item_info):
    return { "error" : message + " " + item_info}



if __name__ == "__main__":
    event_str = '''
    {
    "resource": "/stats/player/{player_guid}",
    "path": "/stats/player/1441314A80B76F",
    "httpMethod": "GET",
    "headers": null,
    "multiValueHeaders": null,
    "queryStringParameters": null,
    "multiValueQueryStringParameters": null,
    "pathParameters": {
        "player_guid": "1441314A80B76F"
    },
    "stageVariables": null,
    "requestContext": {
        "resourceId": "gloe7l",
        "resourcePath": "/stats/player/{player_guid}",
        "httpMethod": "GET",
        "extendedRequestId": "aC0wJEBxIAMFRkQ=",
        "requestTime": "01/Feb/2021:02:31:28 +0000",
        "path": "/stats/player/{player_guid}",
        "accountId": "123456789012",
        "protocol": "HTTP/1.1",
        "stage": "test-invoke-stage",
        "domainPrefix": "testPrefix",
        "requestTimeEpoch": 1612146688858,
        "requestId": "a3d96cc0-e6b4-4608-9969-56582aa72109",
        "identity": {
            "cognitoIdentityPoolId": null,
            "cognitoIdentityId": null,
            "apiKey": "test-invoke-api-key",
            "principalOrgId": null,
            "cognitoAuthenticationType": null,
            "userArn": "arn:aws:iam::123456789012:user/userx",
            "apiKeyId": "test-invoke-api-key-id",
            "userAgent": "aws-internal/3 aws-sdk-java/1.11.901 Linux/4.9.230-0.1.ac.223.84.332.metal1.x86_64 OpenJDK_64-Bit_Server_VM/25.275-b01 java/1.8.0_275 vendor/Oracle_Corporation",
            "accountId": "123456789012",
            "caller": "AKIDO",
            "sourceIp": "test-invoke-source-ip",
            "accessKey": "ASIAAFRICA",
            "cognitoAuthenticationProvider": null,
            "user": "AIKIDO"
        },
        "domainName": "testPrefix.testDomainName",
        "apiId": "7vlreeono1"
    },
    "body": null,
    "isBase64Encoded": false
}
    '''
    event = json.loads(event_str)
    print(handler(event, None))