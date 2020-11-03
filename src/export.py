import csv
import logging
import os
from typing import TYPE_CHECKING

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


def handler(event, context):
    result = []

    try:
        table = dynamo.Table(os.getenv('TABLE_NAME'))
        bucket = s3.Bucket(os.getenv('BUCKET_NAME'))
        key = os.getenv('OBJECT_KEY')
        bucket_object = s3.Object(bucket.name, key)
        bucket_object_url = f"https://{bucket.name}.s3.amazonaws.com/{key}"

        # If the total number of scanned items exceeds the maximum dataset size limit of 1 MB, the scan stops and
        # results are returned to the user as a LastEvaluatedKey value to continue the scan in a subsequent
        # operation.

        # See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination
        response = table.scan()
        items = response.get('Items', [])

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items = items + response.get('Items')
        else:
            if len(items) > 0:
                field_names = list(items[0].keys())

                filename = f'/tmp/{key}'

                with open(filename, 'w', newline='') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=field_names)

                    writer.writeheader()

                    for item in items:
                        writer.writerow(item)

                bucket_object.upload_file(filename, ExtraArgs={})
                result.append(response)

        logger.info(
            f"Records: {len(items)} (See {bucket_object_url})" if len(items) > 0 else 'Nothing.')
    except Exception as error:
        logger.error(error)

    return result
