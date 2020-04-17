from flask import Flask, request, make_response, jsonify
import json
import requests
import shapely.geometry as sh
from data import DBConnection
import config
from classes import SHO, SNSO
from ml import machine

# Create app
app = Flask(__name__)

def process_sns(sns):
    snso = SNSO(sns)
    snso.update_intersection(ocean_shape)
    db.insert_dict_as_row(*snso.sns_db_row())
    sho = SHO(snso.prod_id)
    if sho.grd: db.insert_dict_as_row(*sho.grd_db_row())
    if sho.ocn: db.insert_dict_as_row(*sho.ocn_db_row())
    # if snso.machinable: # This will reduce the volume of images processed by about 60%
    machine(snso)
    print("shgrd", sho.grd_db_row()[0].get("identifier"))
    print("shocn", sho.ocn_db_row()[0].get("identifier"))
    print("machinable", snso.machinable)
    snso.cleanup()

# Home page
@app.route("/", methods=['GET', 'POST'])
def home():
    sns = {}
    if request.data:
        event = json.loads(request.data)
        sns = event['Records'][0]['Sns']

    mess_type = sns.get('Type')
    if mess_type == None:
        res = {
            "msg": "Not an SNS. No action taken.", 
            "status_code" : 401}
    else:
        # TODO verify the authenticity of a notification https://docs.aws.amazon.com/sns/latest/dg/sns-verify-signature-of-message.html
        pass
    
    if mess_type == 'SubscriptionConfirmation':
        # "UnsubscribeUrl": "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C:a07f782a-5c86-4e96-906d-9347e056b8bc"        r = requests.get(sns.get("SubscribeURL"))
        r = requests.get(sns.get("SubscribeURL"))
        if r.status_code > 200 and r.status_code < 300:
            res = {
                "msg": "Subscribed successfully", 
                "status_code" : 200}
        else:
            res = {
                "msg": "Failed to subscribe", 
                "error": r.text, 
                "status_code" : 406}
    elif mess_type == 'Notification':
        process_sns(sns)
        res = {
            "msg": "Notification processed",
            "Message": json.loads(sns.get("Message")), 
            "status_code" : 200}
    else:
        res = {
            "msg":f"Unknown request type {mess_type}. No action taken.", 
            "status_code" : 400}
    
    return make_response(jsonify(res), res["status_code"])

if __name__ in ["__main__", "anyserver"]: # Adding "anyserver" means that this section is entered during vs code debug mode
    db = DBConnection()

    with open("OceanGeoJSON_lowres.geojson") as f:
        ocean_features = json.load(f)["features"]
    ocean_shape = sh.GeometryCollection([sh.shape(feature["geometry"]).buffer(0) for feature in ocean_features])[0]

    app.run(host=config.APP_HOST, debug=config.DEBUG) # port=config.APP_PORT
