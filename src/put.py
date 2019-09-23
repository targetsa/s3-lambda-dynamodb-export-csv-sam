import json
import boto3
import botostubs
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')  # type: botostubs.S3.S3Resource
dynamo = boto3.resource('dynamodb')  # type: botostubs.DynamoDB.DynamodbResource


def handler(event, context):
    events = event.get('Records', [])
    result = {}

    for e in events:
        if 'ObjectCreated' not in e['eventName']:
            continue

        bucket_name = e['s3']['bucket']['name']
        key = e['s3']['object']['key']
        object_ = s3.Object(bucket_name, key)

        try:
            table_name = os.getenv('TABLE_NAME')
            table = dynamo.Table(table_name)

            (name, ext) = object_.key.split('/')[-1:][0].split('.')
            filename = object_.key.split('/')[-1:][0]
            url = 'https://%s.s3.amazonaws.com/%s' % (bucket_name, key)
            content_type = object_.content_type
            modified_at = object_.last_modified.isoformat()
            found = table.get_item(Key={'name': name}, ProjectionExpression='created_at').get('Item')
            created_at = object_.last_modified.isoformat() if found is None else found.get('created_at')

            item = dict(name=name, ext=ext, filename=filename, content_type=content_type, url=url,
                        created_at=created_at,
                        modified_at=modified_at)

            response = table.put_item(
                Item=item,
                ReturnValues='ALL_OLD'
            )

            result = {
                'Event': e,
                'Response': response
            }
        except Exception as error:
            logger.error(error)

    logger.info(json.dumps(result))

    return {
        'status': 200,
        'body': json.dumps(result)
    }
