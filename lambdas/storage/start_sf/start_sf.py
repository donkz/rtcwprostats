import json


def handler(event, context):
    print('request: {}'.format(json.dumps(event)))
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': 'Save this shit'
    }
        
test_event_string = """
{
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "us-east-1",
            "eventTime": "2021-01-08T07:48:39.738Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": "AWS:xxxx:i-xxxx"
            },
            "requestParameters": {
                "sourceIPAddress": "999.999.999.222"
            },
            "responseElements": {
                "x-amz-request-id": "F99",
                "x-amz-id-2": "IV67Re5"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "OTNhMm",
                "bucket": {
                    "name": "rtcwprostats",
                    "ownerIdentity": {
                        "principalId": "A2"
                    },
                    "arn": "arn:aws:s3:::rtcwprostats"
                },
                "object": {
                    "key": "intake/gameStats_match_1609818887_round_2_te_nordic_b2.json",
                    "size": 70531,
                    "eTag": "88281849bfc62ecc74e9e7331060be3d",
                    "versionId": "g5a.gcOoTjY9w6280NpKmXw8D7ujPV79",
                    "sequencer": "005FF80E5AE76221A5"
                }
            }
        }
    ]
}
"""
test_event = json.loads(test_event_string)