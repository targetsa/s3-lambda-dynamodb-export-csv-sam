import json
import logging
import os
from typing import TYPE_CHECKING
from urllib.parse import unquote_plus

import boto3

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
else:
    S3ServiceResource = object
    DynamoDBServiceResource = object

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo: DynamoDBServiceResource = boto3.resource('dynamodb')


def handler(event, context):
    events = event.get('Records', [])
    result = []

    for e in events:
        logger.info(json.dumps(e))

        if 'ObjectRemoved' not in e['eventName']:
            continue

        object_key = e['s3']['object']['key']
        key = unquote_plus(object_key)

        try:
            table = dynamo.Table(os.getenv('TABLE_NAME'))
            key = dict(key=key)
            response = table.delete_item(Key=key, ReturnValues='ALL_OLD')

            result.append(response)
        except Exception as error:
            logger.error(error)

    logger.info(json.dumps(result))

    return result
