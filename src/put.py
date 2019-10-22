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
    result = []

    for e in events:
        logger.info(json.dumps(e))

        if 'ObjectCreated' not in e['eventName']:
            continue

        bucket_name = e['s3']['bucket']['name']
        key = e['s3']['object']['key']
        object_ = s3.Object(bucket_name, key)

        try:
            table_name = os.getenv('TABLE_NAME')
            table = dynamo.Table(table_name)

            key_split = object_.key.split('/')
            filename = key_split[0] if len(key_split) == 1 else key_split[-1:][0]
            name = key_split[0] if len(key_split) > 1 else filename.split('.')[0]
            content_type = object_.content_type
            url = 'https://%s.s3.amazonaws.com/%s' % (bucket_name, key)
            base_url, _ = url.rsplit('/', 1)

            item_found = table.get_item(Key={'key': key}, ProjectionExpression='created_at').get('Item')

            created_at = object_.last_modified.isoformat() if item_found is None else item_found.get('created_at')
            modified_at = object_.last_modified.isoformat()
            item = dict(key=key,
                        name=name,
                        filename=filename,
                        content_type=content_type,
                        base_url=base_url,
                        url=url,
                        created_at=created_at,
                        modified_at=modified_at, )

            response = table.put_item(Item=item, ReturnValues='ALL_OLD')

            result.append(response)
        except Exception as error:
            logger.error(error)

    logger.info(json.dumps(result))

    return result
