from flask import Flask, request, make_response, jsonify
import json
import requests

# Create app
app = Flask(__name__)

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
        print(json.dumps(sns))
        print("=====================")

    mess_type = sns.get('Type')
    if mess_type == None:
        res = {
            "msg": "Not an SNS message. No action taken.", 
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
    app.run(host="0.0.0.0", port=80, debug=True) #, ssl_context=('cert.pem', 'key.pem'))
    # "UnsubscribeUrl": "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C:a07f782a-5c86-4e96-906d-9347e056b8bc"
