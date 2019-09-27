import json
import os

import boto3
import botostubs
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo = boto3.resource('dynamodb')  # type: botostubs.DynamoDB.DynamodbResource


def handler(event, context):
    events = event.get('Records', [])
    result = []

    for e in events:
        logger.info(json.dumps(e))

        if 'ObjectRemoved' not in e['eventName']:
            continue

        key = e['s3']['object']['key']

        try:
            table_name = os.getenv('TABLE_NAME')
            table = dynamo.Table(table_name)

            key = dict(key=key)

            response = table.delete_item(Key=key, ReturnValues='ALL_OLD')

            result.append(response)
        except Exception as error:
            logger.error(error)

    logger.info(json.dumps(result))

    return result
