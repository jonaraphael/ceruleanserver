import json
import requests
from db_connection import DBConnection
from configs import server_config, ml_config
from process import process_sns
import boto3

client = boto3.client("sqs", region_name="eu-central-1")

while True:
    response = client.receive_message(
        QueueUrl=server_config.SQS_URL,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20,
    )

    if response.get("Messages"):
        for msg in response["Messages"]:
            process_sns(json.loads(msg["Body"])["Records"][0]["Sns"])
            client.delete_message(
                QueueUrl=server_config.SQS_URL, ReceiptHandle=msg["ReceiptHandle"]
            )
