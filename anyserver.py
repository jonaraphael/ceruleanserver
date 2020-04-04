from flask import Flask, request, make_response, jsonify
import json
import requests
import shapely.geometry as sh
import psycopg2
from data import DBConnection
from config import Config

# Create app
app = Flask(__name__)

def process_sns(sns):
    pass

def filter_oceans(sns):
    msg = json.loads(sns['Message'])
    # Only process those scenes that include some portion of the ocean
    coords = msg['footprint']['coordinates'][0][0]
    # print(coords)
    scene_poly = sh.polygon.Polygon(coords)

    with open("OceanGeoJSON_lowres.geojson") as f:
        ocean_features = json.load(f)["features"]
    ocean = sh.GeometryCollection([sh.shape(feature["geometry"]).buffer(0) for feature in ocean_features])[0]
    # print(scene_poly.intersects(ocean))
    oceanic = scene_poly.intersects(ocean)
    db.insert_into_image_table(msg, oceanic)
    if oceanic:
        process_sns(sns)

# Home page
@app.route("/", methods=['GET', 'POST'])
def home():
    headers = request.headers
    # print(headers)

    sns = {}
    if request.data:
        # print(request.data)
        event = json.loads(request.data)
        sns = event['Records'][0]['Sns']
        print("---------------------")
        # print(json.dumps(sns))
        # print("=====================")

    mess_type = sns.get('Type')
    if mess_type == None:
        res = {
            "msg": "Not an SNS. No action taken.", 
            "status_code" : 401}
    else:
        # TODO verify the authenticity of a notification https://docs.aws.amazon.com/sns/latest/dg/sns-verify-signature-of-message.html
        pass
    
    if mess_type == 'SubscriptionConfirmation':
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
        filter_oceans(sns)
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
    db = DBConnection(Config.db_host, Config.db_user, Config.db_password, 
    Config.db_database, Config.db_port)

    app.run(Config.app_host, Config.app_port, Config.app_debug) #, ssl_context=('cert.pem', 'key.pem'))
    # "UnsubscribeUrl": "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C:a07f782a-5c86-4e96-906d-9347e056b8bc"
