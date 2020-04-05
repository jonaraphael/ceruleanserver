# %%
import os
import xmltodict
from datetime import datetime
import shapely as sh

class SHO:
    def __init__(self, sns_msg, user, pwd):
        self.grd_id = sns_msg["id"]
        # self.grd_id = "S1B_IW_GRDH_1SDV_20200402T003114_20200402T003139_020958_027C13_47BB"
        self.user = user
        self.pwd = pwd
        self.dir = self.grd_id
        self.isoceanic = None
        self.oceanintersection = None

        self.generic_id = self.grd_id[:7]+"????_?"+self.grd_id[13:-4]+"*"
        self.URLs = {"query_prods" : f"https://scihub.copernicus.eu/apihub/search?rows=100&q=(platformname:Sentinel-1 AND filename:{self.generic_id})",}
        self.download_strs = {"query_prods" : f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document={self.dir}/query_prods_results.xml "{self.URLs["query_prods"]}"',}
        
        os.system(f'mkdir {self.dir}')
        os.system(self.download_strs['query_prods'])
        with open(f'{self.dir}/query_prods_results.xml') as f:
            self.query_prods_res = xmltodict.parse(f.read())
        if self.query_prods_res.get('feed').get('opensearch:totalResults') == '0':
            print(f'ERROR No results found matching {self.grd_id}')
            return
        prods = self.query_prods_res.get('feed').get('entry')
        if isinstance(prods, dict): prods = [prods] # If there's only one product, xmlparser returns a dict instead of a list of dicts
        self.grd_xml = {}
        self.ocn_xml = {}
        for p in prods:
            if 'GRD' in p.get('title'): self.grd_xml = p
            if 'OCN' in p.get('title'): self.ocn_xml = p

        self.grd_shid = self.grd_xml.get('id')
        self.ocn_id = self.ocn_xml.get('title')
        self.ocn_shid = self.ocn_xml.get('id')

        if self.ocn_xml:
            self.URLs["download_ocn"] = f"https://scihubf.copernicus.eu/dhus/odata/v1/Products('{self.ocn_shid}')/%24value"
            self.download_strs["download_ocn"] = f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document={self.dir}/ocn.zip "{self.URLs["download_ocn"]}"'

        self.s3 = {
            "bucket" : f"s3://sentinel-s1-l1c/{sns_msg['path']}/",
        }
        if 'VV' in xml_get(self.grd_xml.get('str'), 'polarisationmode'):
            swath = xml_get(self.grd_xml.get('str'), 'swathidentifier')
            self.s3["grd_tiff"] = f"{self.s3['bucket']}measurement/{swath.lower()}-vv.tiff"
            self.download_strs["download_grd_tiff"] = f'aws s3 cp {self.s3["grd_tiff"]} {self.dir}/vv_grd.tiff --request-payer'

    def __repr__(self):
        return f"<SciHubObject: {self.grd_id}>"

    def download_ocn(self): 
        if not self.download_strs.get("download_ocn"):
            print('ERROR No OCN found for this GRD')
        return os.system(self.download_strs.get("download_ocn"))
    
    def download_grd_tiff(self): 
        if not self.download_strs.get("download_grd_tiff"): 
            print('ERROR No grd tiff found with VV polarization')
        return os.system(self.download_strs.get("download_grd_tiff"))

    def update_intersection(self, geoshape):
        scene_poly = sh.polygon.Polygon(self.sns_msg['footprint']['coordinates'][0][0])
        self.isoceanic = scene_poly.intersects(geoshape)
        self.oceanintersection = scene_poly.intersection(geoshape)
        return self.isoceanic

    def grd_db_row(self):
        res = {
            "GRD_scihub_uuid" : self.grd_shid,
            "GRD_scihub_identifier" : self.grd_id,
            "summary" : self.grd_xml.get('summary'),
            "beginposition" : str_to_dt(xml_get(self.grd_xml.get('date'),'beginposition')),
            "endposition" : str_to_dt(xml_get(self.grd_xml.get('date'), 'endposition')),
            "ingestiondate" : str_to_dt(xml_get(self.grd_xml.get('date'), 'ingestiondate')),
            "missiondatatakeid" : int(xml_get(self.grd_xml.get('int'), 'missiondatatakeid')),
            "orbitnumber" : int(xml_get(self.grd_xml.get('int'), 'orbitnumber')),
            "lastorbitnumber" : int(xml_get(self.grd_xml.get('int'), 'lastorbitnumber')),
            "relativeorbitnumber" : int(xml_get(self.grd_xml.get('int'), 'relativeorbitnumber')),
            "lastrelativeorbitnumber" : int(xml_get(self.grd_xml.get('int'), 'lastrelativeorbitnumber')),
            "sensoroperationalmode" : xml_get(self.grd_xml.get('str'), 'sensoroperationalmode'),
            "swathidentifier" : xml_get(self.grd_xml.get('str'), 'swathidentifier'),
            "orbitdirection" : xml_get(self.grd_xml.get('str'), 'orbitdirection'),
            "producttype" : xml_get(self.grd_xml.get('str'), 'producttype'),
            "timeliness" : xml_get(self.grd_xml.get('str'), 'timeliness'),
            "platformname" : xml_get(self.grd_xml.get('str'), 'platformname'),
            "platformidentifier" : xml_get(self.grd_xml.get('str'), 'platformidentifier'),
            "instrumentname" : xml_get(self.grd_xml.get('str'), 'instrumentname'),
            "instrumentshortname" : xml_get(self.grd_xml.get('str'), 'instrumentshortname'),
            "filename" : xml_get(self.grd_xml.get('str'), 'filename'),
            "format" : xml_get(self.grd_xml.get('str'), 'format'),
            "productclass" : xml_get(self.grd_xml.get('str'), 'productclass'),
            "polarisationmode" : xml_get(self.grd_xml.get('str'), 'polarisationmode'),
            "acquisitiontype" : xml_get(self.grd_xml.get('str'), 'acquisitiontype'),
            "status" : xml_get(self.grd_xml.get('str'), 'status'),
            "size" : xml_get(self.grd_xml.get('str'), 'size'),
            # "footprint" : xml_get(self.grd_xml.get('str'), 'footprint'),
            "footprint" : f"ST_GeomFromGeoJSON('{json.dumps(sns_msg['footprint']['coordinates'][0][0])}')"
            "isoceanic" : self.isoceanic,
            "oceanintersection" : self.oceanintersection, 
            "timestamp" : str_to_dt(xml_get(self.grd_xml.get('date'),'beginposition')).timestamp(),
        }
        return res

    def ocn_db_row(self):
        res = {
            "OCN_scihub_uuid" : self.ocn_shid,
            "OCN_scihub_identifier" : self.ocn_id,
            "producttype" : xml_get(self.ocn_xml.get('str'), 'producttype'),
            "filename" : xml_get(self.ocn_xml.get('str'), 'filename'),
            "size" : xml_get(self.ocn_xml.get('str'), 'size'),
            "GRD_scihub_uuid" : self.grd_shid,
        }
        return res
    
    def cleanup(self):
        os.system(f'rm -f -r {self.dir}')
        return True

def xml_get(lst, a, key1="@name", key2="#text"):
    # from a lst of dcts, find the dct that has key value pair (@name:a), then retrieve the value of (#text:?)
    if lst == None: return None # This is a hack for the case where there is no OCN product. TODO handle absent OCN higher up

    for dct in lst:
        if dct.get(key1) == a:
            return dct.get(key2)
    return None

def str_to_dt(s): return datetime.strptime(s[:-1], '%Y-%m-%dT%H:%M:%S.%f')

sns = {'Records': [{'EventSource': 'aws:sns', 'EventVersion': '1.0', 'EventSubscriptionArn': 'arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C:5f87f97c-5b5c-4010-a025-ccea652d0959', 'Sns': {'Type': 'Notification', 'MessageId': '1e97e182-ad01-5321-ad0e-b73c8e45bd47', 'TopicArn': 'arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C', 'Subject': 'productAdded', 'Message': '{\n  "id" : "S1A_IW_GRDH_1SDV_20200212T112430_20200212T112455_031218_03970B_9627",\n  "path" : "GRD/2020/2/12/IW/DV/S1A_IW_GRDH_1SDV_20200212T112430_20200212T112455_031218_03970B_9627",\n  "missionId" : "S1A",\n  "productType" : "GRD",\n  "mode" : "IW",\n  "polarization" : "DV",\n  "startTime" : "2020-02-12T11:24:30",\n  "stopTime" : "2020-02-12T11:24:55",\n  "absoluteOrbitNumber" : 31218,\n  "missionDataTakeId" : 235275,\n  "productUniqueIdentifier" : "9627",\n  "sciHubIngestion" : "2020-02-12T14:54:46.890Z",\n  "s3Ingestion" : "2020-02-12T15:59:39.655Z",\n  "sciHubId" : "d1278cfd-b143-493d-9ebb-c220b0669168",\n  "footprint" : {\n    "type" : "MultiPolygon",\n    "crs" : {\n      "type" : "name",\n      "properties" : {\n        "name" : "urn:ogc:def:crs:EPSG:8.8.1:4326"\n      }\n    },\n    "coordinates" : [ [ [ [ 102.881004, -2.915022 ], [ 105.141113, -2.420639 ], [ 104.820412, -0.913624 ], [ 102.561264, -1.403244 ], [ 102.881004, -2.915022 ] ] ] ]\n  },\n  "filenameMap" : {\n    "measurement/s1a-iw-grd-vv-20200212t112430-20200212t112455-031218-03970b-001.tiff" : "measurement/iw-vv.tiff",\n    "support/s1-level-1-calibration.xsd" : "support/s1-level-1-calibration.xsd",\n    "support/s1-map-overlay.xsd" : "support/s1-map-overlay.xsd",\n    "preview/map-overlay.kml" : "preview/map-overlay.kml",\n    "annotation/s1a-iw-grd-vh-20200212t112430-20200212t112455-031218-03970b-002.xml" : "annotation/iw-vh.xml",\n    "S1A_IW_GRDH_1SDV_20200212T112430_20200212T112455_031218_03970B_9627.SAFE-report-20200212T141916.pdf" : "report-20200212T141916.pdf",\n    "support/s1-product-preview.xsd" : "support/s1-product-preview.xsd",\n    "support/s1-object-types.xsd" : "support/s1-object-types.xsd",\n    "support/s1-level-1-quicklook.xsd" : "support/s1-level-1-quicklook.xsd",\n    "preview/product-preview.html" : "preview/product-preview.html",\n    "annotation/calibration/calibration-s1a-iw-grd-vh-20200212t112430-20200212t112455-031218-03970b-002.xml" : "annotation/calibration/calibration-iw-vh.xml",\n    "measurement/s1a-iw-grd-vh-20200212t112430-20200212t112455-031218-03970b-002.tiff" : "measurement/iw-vh.tiff",\n    "support/s1-level-1-measurement.xsd" : "support/s1-level-1-measurement.xsd",\n    "support/s1-level-1-product.xsd" : "support/s1-level-1-product.xsd",\n    "support/s1-level-1-noise.xsd" : "support/s1-level-1-noise.xsd",\n    "annotation/calibration/noise-s1a-iw-grd-vv-20200212t112430-20200212t112455-031218-03970b-001.xml" : "annotation/calibration/noise-iw-vv.xml",\n    "annotation/s1a-iw-grd-vv-20200212t112430-20200212t112455-031218-03970b-001.xml" : "annotation/iw-vv.xml",\n    "preview/quick-look.png" : "preview/quick-look.png",\n    "manifest.safe" : "manifest.safe",\n    "annotation/calibration/noise-s1a-iw-grd-vh-20200212t112430-20200212t112455-031218-03970b-002.xml" : "annotation/calibration/noise-iw-vh.xml",\n    "preview/icons/logo.png" : "preview/icons/logo.png",\n    "annotation/calibration/calibration-s1a-iw-grd-vv-20200212t112430-20200212t112455-031218-03970b-001.xml" : "annotation/calibration/calibration-iw-vv.xml"\n  }\n}', 'Timestamp': '2020-02-12T16:00:04.177Z', 'SignatureVersion': '1', 'Signature': 'aAb7LvC+AoINFT+o6C6JGf1cn+5T6uopDH/zMSY9he4CeBo3STOrpXfVlRu+23IJU9ThZtnfG0T3V0qqB6j+XVVEPVccNMtrTjfPlpM3XWq6mmMmQKpsU243qVhzZPpsT0ybP5XthOdvPu6FW+Fe1TY8Seh3Se3RTLyRBzULJRYU81q19uLUniNHmD+8j1pbGeCGFOjeO4Dmbw9H7Adi2m+d18FYC+7p2RK7yBoFQJn6Io7kZyuBGch+Z9aOFQBcoAlNBFvGGZX0qHuaMftp1izdauGLj6ybn0nMvxDok2cF/iZcYhiiuJxTHn4NWdst/OEh8ybhRmefmq3QcwBSZg==', 'SigningCertUrl': 'https://sns.eu-central-1.amazonaws.com/SimpleNotificationService-a86cb10b4e1f29c941702d737128f7b6.pem', 'UnsubscribeUrl': 'https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:214830741341:SentinelS1L1C:5f87f97c-5b5c-4010-a025-ccea652d0959', 'MessageAttributes': {}}}]}
sns_msg = json.loads(sns['Records'][0]['Sns']['Message'])

a = SHO(sns_msg)
a

# %%
