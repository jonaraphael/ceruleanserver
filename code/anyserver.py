from flask import Flask, request, make_response, jsonify
import json
import requests
import shapely.geometry as sh
from data import DBConnection
import config
from SciHub_class import SHO

# Create app
app = Flask(__name__)

def process_sns(sns):
    sho = SHO(json.loads(sns['Message']))
    sho.update_intersection(ocean_shape)
    # add to database
    # if ready, machine learn
    print(json.dumps(sho.grd_db_row(),indent=4))
    print(json.dumps(sho.ocn_db_row(), indent=4))
    print(sho.machinable)
    sho.cleanup()

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

if __name__ == "__main__":    
    db = DBConnection()

    with open("OceanGeoJSON_lowres.geojson") as f:
        ocean_features = json.load(f)["features"]
    ocean_shape = sh.GeometryCollection([sh.shape(feature["geometry"]).buffer(0) for feature in ocean_features])[0]

    app.run(config.APP_HOST, config.APP_PORT, config.DEBUG)
