import os
import xmltodict
from datetime import datetime
import shapely.geometry as sh
import config
import json
import requests

class SHO:
    def __init__(self, sns_msg, user=config.SH_USER, pwd=config.SH_PWD):
        # From SNS
        self.sns_msg = sns_msg
        self.grd_id = sns_msg["id"]
        self.grd_shid = sns_msg['sciHubId']
        self.orbitnumber = sns_msg['absoluteOrbitNumber']
        self.footprint = sns_msg['footprint']
        self.missiondatatakeid = sns_msg['missionDataTakeId']
        self.sensoroperationalmode = sns_msg['mode']
        self.polarisationmode = sns_msg['polarization']
        self.s3Ingestion = sns_msg['s3Ingestion']
        self.ingestiondate = sns_msg['sciHubIngestion']
        self.beginposition = sns_msg['startTime']
        self.endposition = sns_msg['stopTime']
        
        # Calculated
        self.dir = self.grd_id
        self.isvv = 'V' in self.polarisationmode
        self.generic_id = self.grd_id[:7]+"????_?"+self.grd_id[13:-4]+"*"
        self.URLs = {"query_prods" : f"https://{user}:{pwd}@scihub.copernicus.eu/apihub/search?rows=100&q=(platformname:Sentinel-1 AND filename:{self.generic_id})",}
        self.s3 = {"bucket" : f"s3://sentinel-s1-l1c/{sns_msg['path']}/",}

        # Placeholders
        self.isoceanic = None
        self.oceanintersection = None
        self.machinable = None

        if self.isvv: # we don't want to process any polarization other than vv
            self.s3["grd_tiff"] = f"{self.s3['bucket']}measurement/{self.sensoroperationalmode.lower()}-vv.tiff"
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
                self.grd = p if 'GRD' in p.get('title') else None  # This is XML
                self.ocn = p if 'OCN' in p.get('title') else None  # This is XML

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

    def grd_db_row(self):
        res = {
            "GRD_scihub_uuid" : self.grd_shid,
            "GRD_scihub_identifier" : self.grd_id,
            "summary" : self.grd.get('summary'),
            "orbitnumber" : self.orbitnumber,
            "footprint" : f"ST_GeomFromGeoJSON('{json.dumps(self.footprint)}')",
            "missiondatatakeid" : self.missiondatatakeid,
            "sensoroperationalmode" : self.sensoroperationalmode,
            "polarisationmode" : self.polarisationmode,
            "s3Ingestion" : self.s3Ingestion,
            "ingestiondate" : self.ingestiondate,
            "beginposition" : self.beginposition,
            "endposition" : self.endposition,
            "isoceanic" : self.isoceanic,
            "oceanintersection" : f"ST_GeomFromGeoJSON('{json.dumps(self.oceanintersection)}')" if self.isoceanic else "",
            "timestamp" : str_to_dt(self.beginposition).timestamp(),
        }
        if self.grd:
            res.update({
                "swathidentifier" : xml_get(self.grd.get('str'), 'swathidentifier'),
                "orbitdirection" : xml_get(self.grd.get('str'), 'orbitdirection'),
                "producttype" : xml_get(self.grd.get('str'), 'producttype'),
                "timeliness" : xml_get(self.grd.get('str'), 'timeliness'),
                "platformname" : xml_get(self.grd.get('str'), 'platformname'),
                "platformidentifier" : xml_get(self.grd.get('str'), 'platformidentifier'),
                "instrumentname" : xml_get(self.grd.get('str'), 'instrumentname'),
                "instrumentshortname" : xml_get(self.grd.get('str'), 'instrumentshortname'),
                "filename" : xml_get(self.grd.get('str'), 'filename'),
                "format" : xml_get(self.grd.get('str'), 'format'),
                "productclass" : xml_get(self.grd.get('str'), 'productclass'),
                "acquisitiontype" : xml_get(self.grd.get('str'), 'acquisitiontype'),
                "status" : xml_get(self.grd.get('str'), 'status'),
                "size" : xml_get(self.grd.get('str'), 'size'),
            })
        return res

    def ocn_db_row(self):
        res = {}
        if self.ocn:
            res.update({
                "OCN_scihub_uuid" : self.ocn_shid,
                "OCN_scihub_identifier" : self.ocn_id,
                "summary" : self.ocn.get('summary'),
                "producttype" : xml_get(self.ocn.get('str'), 'producttype'),
                "filename" : xml_get(self.ocn.get('str'), 'filename'),
                "size" : xml_get(self.ocn.get('str'), 'size'),
                "GRD_scihub_uuid" : self.grd_shid,
            })
        return res

    def cleanup(self):
        os.system(f'rm -f -r {self.dir}')

def xml_get(lst, a, key1="@name", key2="#text"):
    # from a lst of dcts, find the dct that has key value pair (@name:a), then retrieve the value of (#text:?)
    if lst == None: return None # This is a hack for the case where there is no OCN product. TODO handle absent OCN higher up
    for dct in lst:
        if dct.get(key1) == a:
            return dct.get(key2)
    return None

def str_to_dt(s):
    if 'Z' in s:
        fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    else:
        fmt = '%Y-%m-%dT%H:%M:%S'
    return datetime.strptime(s, fmt)