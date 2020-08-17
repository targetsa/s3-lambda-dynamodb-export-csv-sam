import csv
import json
import logging
import os
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3ServiceResource
    from mypy_boto3_dynamodb import DynamoDBServiceResource
else:
    S3ServiceResource = object
    DynamoDBServiceResource = object

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3: S3ServiceResource = boto3.resource('s3')
dynamo: DynamoDBServiceResource = boto3.resource('dynamodb')


def handler(event, context):
    result = []

    try:
        table = dynamo.Table(os.getenv('TABLE_NAME'))
        bucket = s3.Bucket(os.getenv('BUCKET_NAME'))
        response = table.scan()  # TODO: See paging results doc when exceeding 1 MB limit
        items = response.get('Items', [])

        if len(items) > 0:
            field_names = list(items[0].keys())

            key = os.getenv('OBJECT_KEY', f'{table.name}.csv')
            filename = f'/tmp/{key}'

            with open(filename, 'w', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=field_names)

                writer.writeheader()

                for item in items:
                    writer.writerow(item)

            bucket.upload_file(filename, key, ExtraArgs={})

            result.append(response)
    except Exception as error:
        logger.error(error)

    logger.info(json.dumps(result))

    return result
