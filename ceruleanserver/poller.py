import json
import requests
from data import DBConnection
from classes import SHO, SNSO
from configs import server_config, ml_config
from ml.inference import INFERO
from utils.common import load_ocean_shape
import boto3

client = boto3.client("sqs", region_name="eu-central-1")
db = DBConnection()  # Database Object
ocean_shape = load_ocean_shape()  # Ocean Geometry


def process_sns(snso):
    """Processes the raw SNS received from Sinergise
    
    Arguments:  
        sns {dict} -- contains all the metadata and details of a new satellite image on S3
    """
    db.insert_dict_as_row(*snso.sns_db_row())
    sho = SHO(snso.prod_id)
    if sho.grd:
        db.insert_dict_as_row(*sho.grd_db_row())
    if sho.ocn:
        db.insert_dict_as_row(*sho.ocn_db_row())
    if server_config.DOWNLOAD_GRDS:
        snso.download_grd_tiff()  # Download Large GeoTiff
    if snso.grd_path and server_config.RUN_ML:
        infero = INFERO(snso)
        infero.run_inference()
        if infero.has_geometry:
            db.insert_dict_as_row(*infero.inf_db_row())
            if server_config.UPLOAD_OUTPUTS:
                infero.save_small_to_s3()
                infero.save_poly_to_s3()
    if snso.is_downloaded and server_config.CLEANUP_SNS:
        snso.cleanup()


while True:
    response = client.receive_message(
        QueueUrl=server_config.SQS_URL,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20,
    )

    if response.get("Messages"):
        for msg in response["Messages"]:  # Up to 10 messages
            snso = SNSO(json.loads(msg["Body"])["Records"][0]["Sns"])
            snso.update_intersection(ocean_shape)
            already_processed = (len(db.read_field_from_field_value_table("grd_id", "sns_messageid", f"'{snso.sns['MessageId']}'", "sns"))> 0)
            if snso.is_machinable and (not already_processed or not server_config.BLOCK_REPEAT_SNS):
                process_sns(snso)
            client.delete_message(QueueUrl=server_config.SQS_URL, ReceiptHandle=msg["ReceiptHandle"])
