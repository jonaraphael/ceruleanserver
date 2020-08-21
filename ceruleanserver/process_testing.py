#%%
import pytest
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver"))
from process import process_sns # pylint: disable=no-name-in-module


pid = "S1A_S3_GRDH_1SSV_20200804T220923_20200804T220947_033763_03E9D9_79E3"

test_json = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C:94c822d8-ce87-44b9-bfec-13b2a0a8675b",
            "Sns": {
                "Type": "Notification",
                "MessageId": "09594279-4fa5-56b7-bdce-1568068e0471",
                "TopicArn": "arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C",
                "Subject": "productAdded",
                "Message": f'{{ "id" : "{pid}",  "path" : "GRD/{pid[17:21]}/{str(int(pid[21:23]))}/{str(int(pid[23:25]))}/{pid[4:6]}/{pid[14:16]}/{pid}",  "missionId" : "S1B",  "productType" : "GRD",  "mode" : "IW",  "polarization" : "DV",  "startTime" : "2020-04-15T16:07:06",  "stopTime" : "2020-04-15T16:07:31",  "absoluteOrbitNumber" : 21157,  "missionDataTakeId" : 164441,  "productUniqueIdentifier" : "9E75",  "sciHubIngestion" : "2020-04-15T16:56:40.034Z",  "s3Ingestion" : "2020-04-15T17:39:36.796Z",  "sciHubId" : "034586fd-33e0-4223-89cf-958ae767734f",  "footprint" : {{    "type" : "MultiPolygon",    "crs" : {{      "type" : "name",      "properties" : {{        "name" : "urn:ogc:def:crs:EPSG:8.8.1:4326"      }}    }},    "coordinates" : [ [ [ [ 25.81105, 39.659679 ], [ 28.812616, 40.06459 ], [ 28.49769, 41.565166 ], [ 25.427782, 41.161327 ], [ 25.81105, 39.659679 ] ] ] ]  }},  "filenameMap" : {{    "measurement/s1b-iw-grd-vh-20200406T194140_20200406T194205_032011_03B2AB-002.tiff" : "measurement/iw-vh.tiff",    "support/s1-level-1-calibration.xsd" : "support/s1-level-1-calibration.xsd",    "support/s1-map-overlay.xsd" : "support/s1-map-overlay.xsd",    "annotation/s1b-iw-grd-vh-20200406T194140_20200406T194205_032011_03B2AB-002.xml" : "annotation/iw-vh.xml",    "preview/map-overlay.kml" : "preview/map-overlay.kml",    "annotation/calibration/noise-s1b-iw-grd-vh-20200406T194140_20200406T194205_032011_03B2AB-002.xml" : "annotation/calibration/noise-iw-vh.xml",    "annotation/s1b-iw-grd-vv-20200406T194140_20200406T194205_032011_03B2AB-001.xml" : "annotation/iw-vv.xml",    "support/s1-product-preview.xsd" : "support/s1-product-preview.xsd",    "support/s1-object-types.xsd" : "support/s1-object-types.xsd",    "support/s1-level-1-quicklook.xsd" : "support/s1-level-1-quicklook.xsd",    "preview/product-preview.html" : "preview/product-preview.html",    "measurement/s1b-iw-grd-vv-20200406T194140_20200406T194205_032011_03B2AB-001.tiff" : "measurement/iw-vv.tiff",    "annotation/calibration/calibration-s1b-iw-grd-vh-20200406T194140_20200406T194205_032011_03B2AB-002.xml" : "annotation/calibration/calibration-iw-vh.xml",    "{pid}.SAFE-report-20200415T163033.pdf" : "report-20200415T163033.pdf",    "annotation/calibration/calibration-s1b-iw-grd-vv-20200406T194140_20200406T194205_032011_03B2AB-001.xml" : "annotation/calibration/calibration-iw-vv.xml",    "support/s1-level-1-measurement.xsd" : "support/s1-level-1-measurement.xsd",    "support/s1-level-1-noise.xsd" : "support/s1-level-1-noise.xsd",    "support/s1-level-1-product.xsd" : "support/s1-level-1-product.xsd",    "preview/quick-look.png" : "preview/quick-look.png",    "manifest.safe" : "manifest.safe",    "preview/icons/logo.png" : "preview/icons/logo.png",    "annotation/calibration/noise-s1b-iw-grd-vv-20200406T194140_20200406T194205_032011_03B2AB-001.xml" : "annotation/calibration/noise-iw-vv.xml"  }}}}',
                "Timestamp": "2020-04-15T17:39:55.875Z",
                "SignatureVersion": "1",
                "Signature": "CvhqoizHTIyeanJE2aK+iG1oWWfAI0nQywyE2oIlVUXo7lkpfandmbqRn5ee6D5NWXvvfqzNwpQ/IRUofIchdWADhxwhae5sJZ6axbIfYc7jqyjQZ45G0jlnflstXo3fKFyYZOg1NDbWo52phwIRm8sQ5cDVYAu0ugSXkjm37KWSP59jyQv9KgeHBY6WhRI9iKzGV4wCJ5TTleF6hPoxtZ1MFQaU7/cmHsQc7kkSvpqCFGaMgNa71T+4w0AhSQe1Dd8aw4Cl8e5IiByyFhB+lt7HU8QFA+mhimaCG2iSLNAKK3CqDfKxj692kQiVCEzbuy8R8wb7YWnnTx4QFcG19w==",
                "SigningCertUrl": "https://sns.eu-central-1.amazonaws.com/SimpleNotificationService-a86cb10b4e1f29c941702d737128f7b6.pem",
                "UnsubscribeUrl": "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C:94c822d8-ce87-44b9-bfec-13b2a0a8675b",
                "MessageAttributes": {}
            }
        }
    ]
}

process_sns(test_json['Records'][0]["Sns"])