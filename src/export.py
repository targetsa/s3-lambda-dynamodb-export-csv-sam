import csv
import json
import os

import boto3
import botostubs
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo = boto3.resource('dynamodb')  # type: botostubs.DynamoDB.DynamodbResource
s3 = boto3.resource('s3')  # type: botostubs.S3.S3Resource


def handler(event, context):
    table_name = os.getenv('TABLE_NAME')
    bucket_name = os.getenv('BUCKET_NAME')
    result = []

    try:
        table = dynamo.Table(table_name)
        bucket = s3.Bucket(bucket_name)
        response = table.scan()  # TODO: See paging results doc when exceeding 1 MB limit
        items = response.get('Items', [])
        fieldnames = list(items[0].keys())

        key = 'photos.csv'
        filename = '/tmp/photos.csv'

        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            for item in items:
                writer.writerow(item)

        bucket.upload_file(filename, key)

        result.append(response)
    except Exception as error:
        logger.error(error)

    logger.info(json.dumps(result))

    return {}