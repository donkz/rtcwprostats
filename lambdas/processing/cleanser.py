import json


def handler(event, context):
    """Under construction."""
    print('request: {}'.format(json.dumps(event)))
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': 'Save this shit'
    }  
