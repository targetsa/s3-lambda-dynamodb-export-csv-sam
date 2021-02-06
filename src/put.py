import json
import logging
import os
from typing import TYPE_CHECKING
from urllib.parse import unquote_plus

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import S3ServiceResource
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
else:
    S3ServiceResource = object
    DynamoDBServiceResource = object

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3: S3ServiceResource = boto3.resource('s3')
dynamo: DynamoDBServiceResource = boto3.resource('dynamodb')


# noinspection SpellCheckingInspection
def handler(event, context):
    events = event.get('Records', [])
    result = []

    for e in events:
        logger.info(json.dumps(e))

        if 'ObjectCreated' not in e['eventName']:
            continue

        bucket_name = e['s3']['bucket']['name']
        object_key = e['s3']['object']['key']
        key = unquote_plus(object_key)

        try:
            bucket_object = s3.Object(bucket_name, key)
            table = dynamo.Table(os.getenv('TABLE_NAME'))
            table_item = table.get_item(Key={'key': key}, ProjectionExpression='created_at').get('Item')

            object_path = bucket_object.key.split('/')

            filename = object_path[0] if len(object_path) == 1 else object_path[-1:][0]
            root_directory = object_path[0] if len(object_path) > 1 else ''
            parent_directory = object_path[-2] if len(object_path) > 2 else root_directory

            # Continuar la siguiente iteración si al menos una entrada en `object_path` empieza con un punto; también
            # conocido como archivo o directorio oculto en sistemas Unix.
            if any(df for df in [filename, root_directory, parent_directory] if df.startswith('.')):
                continue

            content_type = bucket_object.content_type
            url = 'https://%s.s3.amazonaws.com/%s' % (bucket_name, object_key)
            base_url, _ = url.rsplit('/', 1)
            created_at = bucket_object.last_modified.isoformat() if table_item is None else table_item.get('created_at')
            modified_at = bucket_object.last_modified.isoformat()

            item = dict(key=key,
                        filename=filename,
                        root_directory=root_directory,
                        parent_directory=parent_directory,
                        content_type=content_type,
                        base_url=base_url,
                        url=url,
                        created_at=created_at,
                        modified_at=modified_at)

            result.append(table.put_item(Item=item, ReturnValues='ALL_OLD'))
        except Exception as error:
            logger.error(error)

    logger.info(json.dumps(result))

    return result
