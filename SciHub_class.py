import os
import xmltodict
from datetime import datetime
import shapely.geometry as sh
import json
from config import Config

class SHO:
    def __init__(self, sns_msg, user=Config.sh_user, pwd=Config.sh_pwd):
        self.sns_msg = sns_msg
        self.grd_id = self.sns_msg["id"]
        self.user = user
        self.pwd = pwd
        self.dir = self.grd_id
        self.isoceanic = None
        self.oceanintersection = None
        self.grd = {}
        self.ocn = {}
        self.grd_shid = None
        self.ocn_id =  None
        self.ocn_shid =  None

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
        for p in prods:
            if 'GRD' in p.get('title'): self.grd = p # This is XML
            if 'OCN' in p.get('title'): self.ocn = p # This is XML

        self.grd_shid = self.grd.get('id')
        self.ocn_id = self.ocn.get('title')
        self.ocn_shid = self.ocn.get('id')

        if self.ocn:
            self.URLs["download_ocn"] = f"https://scihubf.copernicus.eu/dhus/odata/v1/Products('{self.ocn_shid}')/%24value"
            self.download_strs["download_ocn"] = f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document={self.dir}/ocn.zip "{self.URLs["download_ocn"]}"'

        self.s3 = {
            "bucket" : f"s3://sentinel-s1-l1c/{sns_msg['path']}/",
        }
        if 'VV' in xml_get(self.grd.get('str'), 'polarisationmode'):
            swath = xml_get(self.grd.get('str'), 'swathidentifier')
            self.s3["grd_tiff"] = f"{self.s3['bucket']}measurement/{swath.lower()}-vv.tiff"
            self.download_strs["download_grd_tiff"] = f'aws s3 cp {self.s3["grd_tiff"]} {self.dir}/vv_grd.tiff --request-payer'

    def __repr__(self):
        return f"<SciHubObject: {self.grd_id}>"

    def download_ocn(self):
        if not self.ocn:
            print('ERROR No OCN found for this GRD')
        else:
            os.system(self.download_strs.get("download_ocn"))
    
    def download_grd_tiff(self): 
        if not (self.grd and self.download_strs.get("download_grd_tiff")): 
            print('ERROR No grd tiff found with VV polarization')
        else:
            os.system(self.download_strs.get("download_grd_tiff"))

    def update_intersection(self, ocean_shape):
        scene_poly = sh.polygon.Polygon(self.sns_msg['footprint']['coordinates'][0][0])
        self.isoceanic = scene_poly.intersects(ocean_shape)
        inter = scene_poly.intersection(ocean_shape)
        self.oceanintersection = {k: sh.mapping(inter).get(k, v) for k, v in self.sns_msg['footprint'].items()} # use msg[footprint] projection, and overwrite the intersection on top of the previous coordinates

    def grd_db_row(self):
        if self.grd:
            res = {
                "GRD_scihub_uuid" : self.grd_shid,
                "GRD_scihub_identifier" : self.grd_id,
                "summary" : self.grd.get('summary'),
                "beginposition" : str_to_dt(xml_get(self.grd.get('date'),'beginposition')),
                "endposition" : str_to_dt(xml_get(self.grd.get('date'), 'endposition')),
                "ingestiondate" : str_to_dt(xml_get(self.grd.get('date'), 'ingestiondate')),
                "missiondatatakeid" : int(xml_get(self.grd.get('int'), 'missiondatatakeid')),
                "orbitnumber" : int(xml_get(self.grd.get('int'), 'orbitnumber')),
                "lastorbitnumber" : int(xml_get(self.grd.get('int'), 'lastorbitnumber')),
                "relativeorbitnumber" : int(xml_get(self.grd.get('int'), 'relativeorbitnumber')),
                "lastrelativeorbitnumber" : int(xml_get(self.grd.get('int'), 'lastrelativeorbitnumber')),
                "sensoroperationalmode" : xml_get(self.grd.get('str'), 'sensoroperationalmode'),
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
                "polarisationmode" : xml_get(self.grd.get('str'), 'polarisationmode'),
                "acquisitiontype" : xml_get(self.grd.get('str'), 'acquisitiontype'),
                "status" : xml_get(self.grd.get('str'), 'status'),
                "size" : xml_get(self.grd.get('str'), 'size'),
                "footprint" : f"ST_GeomFromGeoJSON('{json.dumps(self.sns_msg['footprint'])}')",
                "isoceanic" : self.isoceanic,
                "oceanintersection" : f"ST_GeomFromGeoJSON('{json.dumps(self.oceanintersection)}')" if self.isoceanic else "",
                "timestamp" : str_to_dt(xml_get(self.grd.get('date'),'beginposition')).timestamp(),
            }
        else:
            res = {}
        return res

    def ocn_db_row(self):
        if self.ocn:
            res = {
                "OCN_scihub_uuid" : self.ocn_shid,
                "OCN_scihub_identifier" : self.ocn_id,
                "producttype" : xml_get(self.ocn.get('str'), 'producttype'),
                "filename" : xml_get(self.ocn.get('str'), 'filename'),
                "size" : xml_get(self.ocn.get('str'), 'size'),
                "GRD_scihub_uuid" : self.grd_shid,
            }
        else:
            res = {}
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

def str_to_dt(s): return datetime.strptime(s[:-1], '%Y-%m-%dT%H:%M:%S.%f')