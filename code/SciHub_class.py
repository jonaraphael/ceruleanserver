import os
import xmltodict
from datetime import datetime
import shapely.geometry as sh
import config
import json
import requests

class SHO:
    def __init__(self, sns, user=config.SH_USER, pwd=config.SH_PWD):
        # From SNS
        self.sns = sns
        self.sns_msg = json.loads(sns['Message'])
        self.grd_id = self.sns_msg["id"]
        
        # Calculated
        self.dir = self.grd_id
        self.isvv = 'V' in self.sns_msg['polarization']
        self.generic_id = self.grd_id[:7]+"????_?"+self.grd_id[13:-4]+"*"
        self.URLs = {"query_prods" : f"https://{user}:{pwd}@scihub.copernicus.eu/apihub/search?rows=100&q=(platformname:Sentinel-1 AND filename:{self.generic_id})",}
        self.s3 = {"bucket" : f"s3://sentinel-s1-l1c/{self.sns_msg['path']}/",}

        # Placeholders
        self.grd = {}
        self.ocn = {}
        self.isoceanic = None
        self.oceanintersection = None
        self.machinable = None

        if self.isvv: # we don't want to process any polarization other than vv
            self.s3["grd_tiff"] = f"{self.s3['bucket']}measurement/{self.sns_msg['mode'].lower()}-vv.tiff"
            self.s3["grd_tiff_download_str"] = f'aws s3 cp {self.s3["grd_tiff"]} {self.dir}/vv_grd.tiff --request-payer'
        
        with requests.Session() as s:
            p = s.post(self.URLs["query_prods"])
            self.query_prods_res = xmltodict.parse(p.text)
        
        if self.query_prods_res.get('feed').get('opensearch:totalResults') == '0':
            # There are no products listed! https://app.asana.com/0/1170930608885369/1171069537674895
            # The result is that the GRD data will be less complete
            self.onscihub = False
            print(f'WARNING No SciHub results found matching {self.grd_id}')
        else:
            self.onscihub = True
            prods = self.query_prods_res.get('feed').get('entry')
            if isinstance(prods, dict): prods = [prods] # If there's only one product, xmlparser returns a dict instead of a list of dicts
            for p in prods:
                self.grd = p if 'GRD' in p.get('title') else self.grd  # This is XML
                self.ocn = p if 'OCN' in p.get('title') else self.ocn  # This is XML
            
            if self.ocn:
                self.ocn_id = self.ocn.get('title')
                self.ocn_shid = self.ocn.get('id')
                self.URLs["download_ocn"] = f"https://{user}:{pwd}@scihub.copernicus.eu/dhus/odata/v1/Products('{self.ocn_shid}')/%24value"

    def __repr__(self):
        return f"<SciHubObject: {self.grd_id}>"

    def download_ocn(self):
        if not self.ocn:
            print('ERROR No OCN found for this GRD')
        else:
            if not os.path.exists(self.dir):
                os.mkdir(self.dir)
            with requests.Session() as s:
                p = s.get(self.URLs.get("download_ocn"))
                open(f'{self.dir}/ocn.zip', 'wb').write(p.content)            
    
    def download_grd_tiff(self): 
        if not self.s3["grd_tiff"]:
            print('ERROR No grd tiff found with VV polarization')
        else:
            if not os.path.exists(self.dir):
                os.mkdir(self.dir)
            os.system(self.s3["grd_tiff_download_str"])

    def update_intersection(self, ocean_shape):
        scene_poly = sh.polygon.Polygon(self.sns_msg['footprint']['coordinates'][0][0])
        self.isoceanic = scene_poly.intersects(ocean_shape)
        inter = scene_poly.intersection(ocean_shape)
        self.oceanintersection = {k: sh.mapping(inter).get(k, v) for k, v in self.sns_msg['footprint'].items()} # use msg[footprint] projection, and overwrite the intersection on top of the previous coordinates
        self.machinable = self.isoceanic and self.isvv

    def sns_db_row(self):
        tbl = 'sns'
        row = {
            "SNS_MessageId" : f"'{self.sns['MessageId']}'", # Primary Key
            "SNS_Subject" : f"'{self.sns['Subject']}'",
            "SNS_Timestamp" : f"{str_to_ts(self.sns['Timestamp'])}",
            "GRD_id" : f"'{self.sns_msg['id']}'",
            "GRD_sciHubId" : f"'{self.sns_msg['sciHubId']}'", # Unique Constraint
            "absoluteOrbitNumber" : f"{self.sns_msg['absoluteOrbitNumber']}",
            "footprint" : f"ST_GeomFromGeoJSON('{json.dumps(self.sns_msg['footprint'])}')",
            "mode" : f"'{self.sns_msg['mode']}'",
            "polarization" : f"'{self.sns_msg['polarization']}'",
            "s3Ingestion" : f"{str_to_ts(self.sns_msg['s3Ingestion'])}",
            "sciHubIngestion" : f"{str_to_ts(self.sns_msg['sciHubIngestion'])}",
            "startTime" : f"{str_to_ts(self.sns_msg['startTime'])}",
            "stopTime" : f"{str_to_ts(self.sns_msg['stopTime'])}",
            "onscihub" : f"{self.onscihub}",
            "isoceanic" : f"{self.isoceanic}",
            "oceanintersection" : f"ST_GeomFromGeoJSON('{json.dumps(self.oceanintersection)}')" if self.isoceanic else 'null',
        }
        return (row, tbl)

    def grd_db_row(self):
        tbl = 'shgrd'
        row = {}
        if self.grd: # SciHub has additional information
            row.update({
                "SNS_GRD_id" : f"'{self.grd_id}'", # Foreign Key
                "summary" : f"'{self.grd.get('summary')}'",
                "beginposition" : f"{str_to_ts(xml_get(self.grd.get('date'),'beginposition'))}",
                "endposition" : f"{str_to_ts(xml_get(self.grd.get('date'), 'endposition'))}",
                "ingestiondate" : f"{str_to_ts(xml_get(self.grd.get('date'), 'ingestiondate'))}",
                "missiondatatakeid" : f"{int(xml_get(self.grd.get('int'), 'missiondatatakeid'))}",
                "orbitnumber" : f"{int(xml_get(self.grd.get('int'), 'orbitnumber'))}",
                "lastorbitnumber" : f"{int(xml_get(self.grd.get('int'), 'lastorbitnumber'))}",
                "relativeorbitnumber" : f"{int(xml_get(self.grd.get('int'), 'relativeorbitnumber'))}",
                "lastrelativeorbitnumber" : f"{int(xml_get(self.grd.get('int'), 'lastrelativeorbitnumber'))}",
                "sensoroperationalmode" : f"'{xml_get(self.grd.get('str'), 'sensoroperationalmode')}'",
                "swathidentifier" : f"'{xml_get(self.grd.get('str'), 'swathidentifier')}'",
                "orbitdirection" : f"'{xml_get(self.grd.get('str'), 'orbitdirection')}'",
                "producttype" : f"'{xml_get(self.grd.get('str'), 'producttype')}'",
                "timeliness" : f"'{xml_get(self.grd.get('str'), 'timeliness')}'",
                "platformname" : f"'{xml_get(self.grd.get('str'), 'platformname')}'",
                "platformidentifier" : f"'{xml_get(self.grd.get('str'), 'platformidentifier')}'",
                "instrumentname" : f"'{xml_get(self.grd.get('str'), 'instrumentname')}'",
                "instrumentshortname" : f"'{xml_get(self.grd.get('str'), 'instrumentshortname')}'",
                "filename" : f"'{xml_get(self.grd.get('str'), 'filename')}'",
                "format" : f"'{xml_get(self.grd.get('str'), 'format')}'",
                "productclass" : f"'{xml_get(self.grd.get('str'), 'productclass')}'",
                "polarisationmode" : f"'{xml_get(self.grd.get('str'), 'polarisationmode')}'",
                "acquisitiontype" : f"'{xml_get(self.grd.get('str'), 'acquisitiontype')}'",
                "status" : f"'{xml_get(self.grd.get('str'), 'status')}'",
                "size" : f"'{xml_get(self.grd.get('str'), 'size')}'",
                "footprint" : f"'{xml_get(self.grd.get('str'), 'footprint')}'",
                "identifier" : f"'{xml_get(self.grd.get('str'), 'identifier')}'",
                "uuid" : f"'{xml_get(self.grd.get('str'), 'uuid')}'",
            })
        return (row, tbl)

    def ocn_db_row(self):
        tbl = 'shocn'
        row = {}
        if self.ocn:
            row.update({
                "SNS_GRD_id" : f"'{self.grd_id}'", # Foreign Key
                "uuid" : f"'{self.ocn_shid}'",
                "identifier" : f"'{self.ocn_id}'",
                "summary" : f"'{self.ocn.get('summary')}'",
                "producttype" : f"'{xml_get(self.ocn.get('str'), 'producttype')}'",
                "filename" : f"'{xml_get(self.ocn.get('str'), 'filename')}'",
                "size" : f"'{xml_get(self.ocn.get('str'), 'size')}'",
            })
        return (row, tbl)

    def cleanup(self):
        os.system(f'rm -f -r {self.dir}')

def xml_get(lst, a, key1="@name", key2="#text"):
    # from a lst of dcts, find the dct that has key value pair (@name:a), then retrieve the value of (#text:?)
    if lst == None: return None # This is a hack for the case where there is no OCN product. TODO handle absent OCN higher up
    for dct in lst:
        if dct.get(key1) == a:
            return dct.get(key2)
    return None

def str_to_ts(s):
    if 'Z' in s:
        if '.' in s:
            fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
        else:
            fmt = '%Y-%m-%dT%H:%M:%SZ'
    else:
        fmt = '%Y-%m-%dT%H:%M:%S'
    return datetime.strptime(s, fmt).timestamp()