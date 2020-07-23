""" Testing the functionality of our data objects """
import pytest
import sys
import json
import shutil
from pathlib import Path
import shapely.geometry as sh

sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver"))
sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver" / "ml"))
sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver" / "utils"))
from configs import testing_config, path_config  # pylint: disable=import-error
from classes import SNSO, SHO
from common import clear


@pytest.fixture  # This means treat the following as an object (instead of a function)
def mock_GET_body():
    return json.loads(
        """{
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
                    "Message": "{\\n  \\"id\\" : \\"S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112\\",\\n  \\"path\\" : \\"GRD/2020/4/6/IW/DV/S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112\\",\\n  \\"missionId\\" : \\"S1B\\",\\n  \\"productType\\" : \\"GRD\\",\\n  \\"mode\\" : \\"IW\\",\\n  \\"polarization\\" : \\"DV\\",\\n  \\"startTime\\" : \\"2020-04-15T16:07:06\\",\\n  \\"stopTime\\" : \\"2020-04-15T16:07:31\\",\\n  \\"absoluteOrbitNumber\\" : 21157,\\n  \\"missionDataTakeId\\" : 164441,\\n  \\"productUniqueIdentifier\\" : \\"9E75\\",\\n  \\"sciHubIngestion\\" : \\"2020-04-15T16:56:40.034Z\\",\\n  \\"s3Ingestion\\" : \\"2020-04-15T17:39:36.796Z\\",\\n  \\"sciHubId\\" : \\"034586fd-33e0-4223-89cf-958ae767734f\\",\\n  \\"footprint\\" : {\\n    \\"type\\" : \\"MultiPolygon\\",\\n    \\"crs\\" : {\\n      \\"type\\" : \\"name\\",\\n      \\"properties\\" : {\\n        \\"name\\" : \\"urn:ogc:def:crs:EPSG:8.8.1:4326\\"\\n      }\\n    },\\n    \\"coordinates\\" : [ [ [ [ 25.81105, 39.659679 ], [ 28.812616, 40.06459 ], [ 28.49769, 41.565166 ], [ 25.427782, 41.161327 ], [ 25.81105, 39.659679 ] ] ] ]\\n  },\\n  \\"filenameMap\\" : {\\n    \\"measurement/s1b-iw-grd-vh-20200406T194140_20200406T194205_032011_03B2AB-002.tiff\\" : \\"measurement/iw-vh.tiff\\",\\n    \\"support/s1-level-1-calibration.xsd\\" : \\"support/s1-level-1-calibration.xsd\\",\\n    \\"support/s1-map-overlay.xsd\\" : \\"support/s1-map-overlay.xsd\\",\\n    \\"annotation/s1b-iw-grd-vh-20200406T194140_20200406T194205_032011_03B2AB-002.xml\\" : \\"annotation/iw-vh.xml\\",\\n    \\"preview/map-overlay.kml\\" : \\"preview/map-overlay.kml\\",\\n    \\"annotation/calibration/noise-s1b-iw-grd-vh-20200406T194140_20200406T194205_032011_03B2AB-002.xml\\" : \\"annotation/calibration/noise-iw-vh.xml\\",\\n    \\"annotation/s1b-iw-grd-vv-20200406T194140_20200406T194205_032011_03B2AB-001.xml\\" : \\"annotation/iw-vv.xml\\",\\n    \\"support/s1-product-preview.xsd\\" : \\"support/s1-product-preview.xsd\\",\\n    \\"support/s1-object-types.xsd\\" : \\"support/s1-object-types.xsd\\",\\n    \\"support/s1-level-1-quicklook.xsd\\" : \\"support/s1-level-1-quicklook.xsd\\",\\n    \\"preview/product-preview.html\\" : \\"preview/product-preview.html\\",\\n    \\"measurement/s1b-iw-grd-vv-20200406T194140_20200406T194205_032011_03B2AB-001.tiff\\" : \\"measurement/iw-vv.tiff\\",\\n    \\"annotation/calibration/calibration-s1b-iw-grd-vh-20200406T194140_20200406T194205_032011_03B2AB-002.xml\\" : \\"annotation/calibration/calibration-iw-vh.xml\\",\\n    \\"S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112.SAFE-report-20200415T163033.pdf\\" : \\"report-20200415T163033.pdf\\",\\n    \\"annotation/calibration/calibration-s1b-iw-grd-vv-20200406T194140_20200406T194205_032011_03B2AB-001.xml\\" : \\"annotation/calibration/calibration-iw-vv.xml\\",\\n    \\"support/s1-level-1-measurement.xsd\\" : \\"support/s1-level-1-measurement.xsd\\",\\n    \\"support/s1-level-1-noise.xsd\\" : \\"support/s1-level-1-noise.xsd\\",\\n    \\"support/s1-level-1-product.xsd\\" : \\"support/s1-level-1-product.xsd\\",\\n    \\"preview/quick-look.png\\" : \\"preview/quick-look.png\\",\\n    \\"manifest.safe\\" : \\"manifest.safe\\",\\n    \\"preview/icons/logo.png\\" : \\"preview/icons/logo.png\\",\\n    \\"annotation/calibration/noise-s1b-iw-grd-vv-20200406T194140_20200406T194205_032011_03B2AB-001.xml\\" : \\"annotation/calibration/noise-iw-vv.xml\\"\\n  }\\n}",
                    "Timestamp": "2020-04-15T17:39:55.875Z",
                    "SignatureVersion": "1",
                    "Signature": "CvhqoizHTIyeanJE2aK+iG1oWWfAI0nQywyE2oIlVUXo7lkpfandmbqRn5ee6D5NWXvvfqzNwpQ/IRUofIchdWADhxwhae5sJZ6axbIfYc7jqyjQZ45G0jlnflstXo3fKFyYZOg1NDbWo52phwIRm8sQ5cDVYAu0ugSXkjm37KWSP59jyQv9KgeHBY6WhRI9iKzGV4wCJ5TTleF6hPoxtZ1MFQaU7/cmHsQc7kkSvpqCFGaMgNa71T+4w0AhSQe1Dd8aw4Cl8e5IiByyFhB+lt7HU8QFA+mhimaCG2iSLNAKK3CqDfKxj692kQiVCEzbuy8R8wb7YWnnTx4QFcG19w==",
                    "SigningCertUrl": "https://sns.eu-central-1.amazonaws.com/SimpleNotificationService-a86cb10b4e1f29c941702d737128f7b6.pem",
                    "UnsubscribeUrl": "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C:94c822d8-ce87-44b9-bfec-13b2a0a8675b",
                    "MessageAttributes": {}
                }
            }
        ]
        }"""
    )


@pytest.fixture
def mock_sns(mock_GET_body):
    return mock_GET_body["Records"][0]["Sns"]


@pytest.fixture
def ocean_geojson():
    with open(path_config.LOCAL_DIR + "aux_files/OceanGeoJSON_lowres.geojson") as f:
        ocean_features = json.load(f)["features"]
    ocean = sh.GeometryCollection(
        [sh.shape(feature["geometry"]).buffer(0) for feature in ocean_features]
    )[0]
    return ocean


@pytest.fixture
def FILE_grd_path():
    test_dir = (
        Path(path_config.LOCAL_DIR)
        / testing_config.TESTDATA_FOLDER
        / testing_config.TESTDATA_VERSION
    )
    perm_grd = (
        test_dir
        / "perm/S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112/vv_grd.tiff"
    )
    temp_grd = (
        test_dir
        / "temp/S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112/vv_grd.tiff"
    )
    clear(temp_grd)
    temp_grd.parent.mkdir(parents=True, exist_ok=True)  # pylint: disable=no-member
    shutil.copy2(str(perm_grd), str(temp_grd))
    yield temp_grd
    clear(temp_grd)


@pytest.fixture
def FILE_ocn_path():
    test_dir = (
        Path(path_config.LOCAL_DIR)
        / testing_config.TESTDATA_FOLDER
        / testing_config.TESTDATA_VERSION
    )
    perm_ocn = (
        test_dir
        / "perm/S1A_IW_OCN__2SDV_20200406T194140_20200406T194205_032011_03B2AB_4913/ocn.zip"
    )
    temp_ocn = (
        test_dir
        / "temp/S1A_IW_OCN__2SDV_20200406T194140_20200406T194205_032011_03B2AB_4913/ocn.zip"
    )
    clear(temp_ocn)
    temp_ocn.parent.mkdir(parents=True, exist_ok=True)  # pylint: disable=no-member
    shutil.copy2(str(perm_ocn), str(temp_ocn))
    yield temp_ocn
    clear(temp_ocn)


#%% Test SNSO


def test_snso(mock_sns):
    snso = SNSO(mock_sns)
    assert (
        snso.s3["bucket"]
        == "s3://sentinel-s1-l1c/GRD/2020/4/6/IW/DV/S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112/"
    )
    assert snso.is_machinable == True


@pytest.fixture
def snso(mock_sns):
    return SNSO(mock_sns)


def test_snso_download(snso, FILE_grd_path):
    grd_path = snso.download_grd_tiff(FILE_grd_path)
    assert grd_path.exists()


def test_snso_ocean_intersection(snso, ocean_geojson):
    snso.update_intersection(ocean_geojson)
    assert snso.isoceanic == True


def test_snso_db_row(snso):
    row, tbl = snso.sns_db_row()
    assert tbl == "sns"
    assert row["sns_messageid"] == "'09594279-4fa5-56b7-bdce-1568068e0471'"
    assert row["starttime"] == "'2020-04-15T16:07:06'"


def test_snso_cleanup(snso, FILE_grd_path):
    snso.grd_path = FILE_grd_path
    snso.cleanup()
    assert not FILE_grd_path.exists()


#%% Test SHO


def test_sho():
    sho = SHO("S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112")
    assert isinstance(sho.query_prods_res, dict)
    assert (
        sho.grd_id
        == "S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112"
    )
    assert (
        sho.ocn.pid
        == "S1A_IW_OCN__2SDV_20200406T194140_20200406T194205_032011_03B2AB_4913"
    )


@pytest.fixture
def sho():
    return SHO("S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112")


def test_sho_download_grd(sho, FILE_grd_path):
    grd_path = sho.download_grd_tiff_from_s3(FILE_grd_path)
    assert grd_path.exists()


def test_sho_download_ocn(sho, FILE_ocn_path):
    ocn_path = sho.download_ocn(FILE_ocn_path)
    assert ocn_path.exists()


def test_sho_db_row_grd(sho):
    row, tbl = sho.grd_db_row()
    assert tbl == "shgrd"
    assert row["uuid"] == "'ba921ef7-d586-4d11-908f-dae79d20952c'"


def test_sho_db_row_ocn(sho):
    row, tbl = sho.ocn_db_row()
    assert tbl == "shocn"
    assert row["uuid"] == "'bc036262-186f-4dd6-ab49-21aab0f9031e'"


def test_sho_cleanup(sho, FILE_grd_path, FILE_ocn_path):
    sho.grd_path = FILE_grd_path
    sho.ocn_path = FILE_ocn_path
    sho.cleanup()
    assert not FILE_grd_path.exists()
    assert not FILE_ocn_path.exists()
