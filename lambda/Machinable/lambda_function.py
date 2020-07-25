# zip -r9 ../machinable.zip .
import json
import boto3
import shapely.geometry as sh  # https://docs.aws.amazon.com/lambda/latest/dg/python-package.html


def lambda_handler(event, context):
    print("shapely imported!")
    if event.get("Records"):
        sns = event["Records"][0]["Sns"]
        msg = json.loads(sns["Message"])
        scene_poly = sh.polygon.Polygon(msg["footprint"]["coordinates"][0][0])

        is_highdef = "H" == msg["id"][10]
        is_vv = "V" == msg["id"][15] # we don't want to process any polarization other than vv XXX This is hardcoded in the server, where we look for a vv.grd file
        is_oceanic = scene_poly.intersects(ocean_poly)

        if is_highdef and is_vv and is_oceanic:
            client = boto3.client("sqs", region_name="eu-central-1")
            response = client.send_message(
                QueueUrl="https://sqs.eu-central-1.amazonaws.com/162277344632/New_Machinable",
                MessageBody=json.dumps(event),
            )

        # sns_db_row(sns['MessageId'], sns['Subject'], sns['Timestamp'], msg, is_oceanic)
        # TODO: insert SNS into DB
        # https://docs.aws.amazon.com/lambda/latest/dg/services-rds-tutorial.html

        return {"statusCode": 200}
    else:
        return {"statusCode": 400}


def load_ocean_poly():
    with open("OceanGeoJSON_lowres.geojson") as f:
        ocean_features = json.load(f)["features"]
    geom = sh.GeometryCollection(
        [sh.shape(feature["geometry"]).buffer(0) for feature in ocean_features]
    )[0]
    return geom


def sns_db_row(sns_id, sns_sub, sns_ts, msg, is_oceanic):
    # Warning! PostgreSQL hates capital letters, so the keys are different between the SNS and the DB
    """Creates a dictionary that aligns with our SNS DB columns
    
    Returns:
        dict -- key for each column in our SNS DB, value from this SNS's content
    """
    tbl = "sns"
    row = {
        "sns_messageid": f"'{sns_id}'",  # Primary Key
        "sns_subject": f"'{sns_sub}'",
        "sns_timestamp": f"'{sns_ts}'",
        "grd_id": f"'{msg['id']}'",
        "grd_uuid": f"'{msg['sciHubId']}'",  # Unique Constraint
        "absoluteorbitnumber": f"{msg['absoluteOrbitNumber']}",
        "footprint": f"ST_GeomFromGeoJSON('{json.dumps(msg['footprint'])}')",
        "mode": f"'{msg['mode']}'",
        "polarization": f"'{msg['polarization']}'",
        "s3ingestion": f"'{msg['s3Ingestion']}'",
        "scihubingestion": f"'{msg['sciHubIngestion']}'",
        "starttime": f"'{msg['startTime']}'",
        "stoptime": f"'{msg['stopTime']}'",
        "isoceanic": f"{is_oceanic}",
    }
    return (row, tbl)


ocean_poly = load_ocean_poly()
# db =

