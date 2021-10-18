import boto3
import json

def clean_policy(fn_name):
    client = boto3.client('lambda') 
    policy = client.get_policy(FunctionName=fn_name)['Policy']
    statements = json.loads(policy)['Statement'] 
    sid_list = [item['Sid'] for item in statements][:-1]
    for sid in sid_list:       
        print("Removing policy SID {}".format(sid))
        client.remove_permission(FunctionName=fn_name, StatementId=sid)
    print(client.get_policy(FunctionName=fn_name))
    
clean_policy("kek")

# saving for later; did not test

