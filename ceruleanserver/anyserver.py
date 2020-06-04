from flask import Flask, request, make_response, jsonify
import json
import requests
from data import DBConnection
from classes import SHO, SNSO
from configs import server_config, ml_config
from ml.inference import INFERO
from utils.common import load_ocean_shape

# Create app
app = Flask(__name__)


def process_sns(sns):
    """Processes the raw SNS received from Sinergise
    
    Arguments:  
        sns {dict} -- contains all the metadata and details of a new satellite image on S3
    """
    snso = SNSO(sns)
    snso.update_intersection(ocean_shape)
    db.insert_dict_as_row(*snso.sns_db_row())
    sho = SHO(snso.prod_id)
    if sho.grd:
        db.insert_dict_as_row(*sho.grd_db_row())
    if sho.ocn:
        db.insert_dict_as_row(*sho.ocn_db_row())
    if snso.is_machinable and server_config.DOWNLOAD_GRDS:
        snso.download_grd_tiff()  # Download Large GeoTiff
    if snso.grd_path and server_config.RUN_ML:
        infero = INFERO(snso.grd_path, snso.prod_id)
        infero.run_inference()
        if infero.has_geometry:
            db.insert_dict_as_row(*infero.inf_db_row())
    if server_config.CLEANUP_SNS:
        snso.cleanup()


# Home page
@app.route("/", methods=["GET", "POST"])
def home():
    """The function called by Lambda when forwarding an SNS
    
    Returns:
        HTTP Reponse -- 200 if successful
    """
    sns = {}
    if request.data:
        event = json.loads(request.data)
        sns = event["Records"][0]["Sns"]

    mess_type = sns.get("Type")
    if mess_type == None:
        res = {"msg": "Not an SNS. No action taken.", "status_code": 401}
    else:
        # TODO verify the authenticity of a notification https://docs.aws.amazon.com/sns/latest/dg/sns-verify-signature-of-message.html
        pass

    if mess_type == "SubscriptionConfirmation":
        # "UnsubscribeUrl": "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C:a07f782a-5c86-4e96-906d-9347e056b8bc"        r = requests.get(sns.get("SubscribeURL"))
        r = requests.get(sns.get("SubscribeURL"))
        if r.status_code > 200 and r.status_code < 300:
            res = {"msg": "Subscribed successfully", "status_code": 200}
        else:
            res = {"msg": "Failed to subscribe", "error": r.text, "status_code": 406}
    elif mess_type == "Notification":
        process_sns(
            sns
        )  # XXXBen Is it possible to return success before deploying resources to process the content of the SNS?
        res = {
            "msg": "Notification processed",
            "Message": json.loads(sns.get("Message")),
            "status_code": 200,
        }
    else:
        res = {
            "msg": f"Unknown request type {mess_type}. No action taken.",
            "status_code": 400,
        }

    return make_response(jsonify(res), res["status_code"])


if __name__ in [
    "__main__",
    "anyserver",
]:  # Adding "anyserver" means that this section is entered during vs code debug mode
    # Permanent Objects:
    db = DBConnection()  # Database Object
    ocean_shape = load_ocean_shape()  # Ocean Geometry
    # Start listening on the default port
    app.run(host=server_config.APP_HOST, debug=server_config.FLASK_DEBUG)
