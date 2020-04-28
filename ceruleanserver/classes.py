import os
import xmltodict
from datetime import datetime
import shapely.geometry as sh
import config
import json
import requests
import shutil
from pathlib import Path

class SNSO:
    """A Class that organizes information in the SNS
    """    
    def __init__(self, sns):
        # From SNS
        self.sns = sns
        self.sns_msg = json.loads(sns['Message'])
        self.prod_id = self.sns_msg["id"]
        
        # Calculated
        self.is_vv = 'V' in self.sns_msg['polarization']
        self.s3 = {"bucket" : f"s3://sentinel-s1-l1c/{self.sns_msg['path']}/",}
        self.dir = self.prod_id

        # Placeholders
        self.isoceanic = None
        self.oceanintersection = None
        self.machinable = self.is_vv # Later amended to include isoceanic
        
        if self.is_vv: # we don't want to process any polarization other than vv
            self.s3["grd_tiff"] = f"{self.s3['bucket']}measurement/{self.sns_msg['mode'].lower()}-vv.tiff"
            self.s3["grd_tiff_dest"] = f"{self.dir}/vv_grd.tiff"
            self.s3["grd_tiff_download_str"] = f'aws s3 cp {self.s3["grd_tiff"]} {self.s3["grd_tiff_dest"]} --request-payer'
        
    def __repr__(self):
        return f"<SNSObject: {self.sns_msg['id']}>"          

    def download_grd_tiff(self):
        """Creates a local directory and downloads a GeoTiff (often ~700MB)
        """
        if config.VERBOSE: print('Downloading GRD Tiff')
        if not self.s3["grd_tiff"]:
            print('ERROR No grd tiff found with VV polarization')
        else:
            Path(self.dir).mkdir(exist_ok=True)
            if not Path(self.s3["grd_tiff_dest"]).exists():
                os.system(self.s3["grd_tiff_download_str"])

    def cleanup(self):
        """Delete any local directory made to store the GRD
        """        
        if Path(self.dir).exists():
            Path(self.dir).unlink()

    def update_intersection(self, ocean_shape):
        """Calculate geometric intersections with the supplied geometry
        
        Arguments:
            ocean_shape {shapely geom} -- built from a geojson of the worlds oceans
        """
        scene_poly = sh.polygon.Polygon(self.sns_msg['footprint']['coordinates'][0][0])
        self.isoceanic = scene_poly.intersects(ocean_shape)
        inter = scene_poly.intersection(ocean_shape)
        self.oceanintersection = {k: sh.mapping(inter).get(k, v) for k, v in self.sns_msg['footprint'].items()} # use msg[footprint] projection, and overwrite the intersection on top of the previous coordinates
        self.machinable = self.isoceanic and self.is_vv

    def sns_db_row(self): # Warning! PostgreSQL hates capital letters, so the keys are different between the SNS and the DB
        """Creates a dictionary that aligns with our SNS DB columns
        
        Returns:
            dict -- key for each column in our SNS DB, value from this SNS's content
        """        
        tbl = 'sns'
        row = {
            "sns_messageid" : f"'{self.sns['MessageId']}'", # Primary Key
            "sns_subject" : f"'{self.sns['Subject']}'",
            "sns_timestamp" : f"{str_to_ts(self.sns['Timestamp'])}",
            "grd_id" : f"'{self.sns_msg['id']}'",
            "grd_uuid" : f"'{self.sns_msg['sciHubId']}'", # Unique Constraint
            "absoluteorbitnumber" : f"{self.sns_msg['absoluteOrbitNumber']}",
            "footprint" : f"ST_GeomFromGeoJSON('{json.dumps(self.sns_msg['footprint'])}')",
            "mode" : f"'{self.sns_msg['mode']}'",
            "polarization" : f"'{self.sns_msg['polarization']}'",
            "s3ingestion" : f"{str_to_ts(self.sns_msg['s3Ingestion'])}",
            "scihubingestion" : f"{str_to_ts(self.sns_msg['sciHubIngestion'])}",
            "starttime" : f"{str_to_ts(self.sns_msg['startTime'])}",
            "stoptime" : f"{str_to_ts(self.sns_msg['stopTime'])}",
            "isoceanic" : f"{self.isoceanic}",
            "oceanintersection" : f"ST_GeomFromGeoJSON('{json.dumps(self.oceanintersection)}')" if self.isoceanic else 'null',
        }
        return (row, tbl)

class SHO:
    """A class that organizes information about content stored on SciHub
    """    
    def __init__(self, prod_id, user=config.SH_USER, pwd=config.SH_PWD):
        self.prod_id = prod_id
        self.generic_id = self.prod_id[:7]+"????_?"+self.prod_id[13:-4]+"*"
        self.URLs = {"query_prods" : f"https://{user}:{pwd}@scihub.copernicus.eu/apihub/search?q=(platformname:Sentinel-1 AND filename:{self.generic_id})",}

        # Placeholders
        self.grd = {}
        self.ocn = {}
        self.grd_id = None
        self.grd_shid = None
        self.ocn_id = None
        self.ocn_shid = None
        self.dir = None
        
        with requests.Session() as s:
            p = s.post(self.URLs["query_prods"])
            self.query_prods_res = xmltodict.parse(p.text)
        
        if self.query_prods_res.get('feed').get('opensearch:totalResults') == '0':
            # There are no products listed! https://app.asana.com/0/1170930608885369/1171069537674895
            # The result is that the GRD data will be less complete
            print(f'WARNING No SciHub results found matching {self.generic_id}')
        else:
            prods = self.query_prods_res.get('feed').get('entry')
            if isinstance(prods, dict): prods = [prods] # If there's only one product, xmlparser returns a dict instead of a list of dicts
            for p in prods:
                self.grd = p if 'GRD' in p.get('title') else self.grd  # This is XML
                self.ocn = p if 'OCN' in p.get('title') else self.ocn  # This is XML
            
            if self.grd:
                self.grd_id = self.grd.get('title')
                self.grd_shid = self.grd.get('id')
            
            if self.ocn:
                self.ocn_id = self.ocn.get('title')
                self.dir = self.ocn_id
                self.ocn_shid = self.ocn.get('id')
                self.URLs["download_ocn"] = f"https://{user}:{pwd}@scihub.copernicus.eu/dhus/odata/v1/Products('{self.ocn_shid}')/%24value"

    def __repr__(self):
        return f"<SciHubObject: {self.prod_id}>"
    
    def download_grd_tiff_from_s3(self):
        """Create a local directory, and download a GRD file to it if it exists in Sinergise's archives
        """
        if config.VERBOSE: print('Downloading GRD')
        if not self.grd:
            print('ERROR No GRD found for this product ID')
        else:
            self.s3 = {"path" : self.prod_id[7:10]+'/'+self.prod_id[17:21]+'/'+str(int(self.prod_id[21:23]))+'/'+str(int(self.prod_id[23:25]))+'/'+self.prod_id[4:6]+'/'+self.prod_id[14:16]+'/'+self.prod_id,}
            mode = f"'{xml_get(self.grd.get('str'), 'swathidentifier')}'"
            self.s3["bucket"] = f"s3://sentinel-s1-l1c/{self.s3['path']}/"
            self.s3["grd_tiff"] = f"{self.s3['bucket']}measurement/{mode.lower()}-vv.tiff"
            self.s3["grd_tiff_dest"] = f"{self.grd_id}/vv_grd.tiff"
            self.s3["grd_tiff_download_str"] = f'aws s3 cp {self.s3["grd_tiff"]} {self.s3["grd_tiff_dest"]} --request-payer'
            Path(self.grd_id).mkdir(exist_ok=True)
            if not Path(self.s3["grd_tiff_dest"]).exists():
                os.system(self.s3["grd_tiff_download_str"])

    def download_ocn(self):
        """Create a local directory, and download an OCN zip file to it
        """
        if config.VERBOSE: print('Downloading OCN')
        if not self.ocn:
            print('ERROR No OCN found for this GRD')
        else:
            Path(self.dir).mkdir(exist_ok=True)
            with requests.Session() as s:
                p = s.get(self.URLs.get("download_ocn"))
                open(f'{self.dir}/ocn.zip', 'wb').write(p.content)

    def cleanup(self):
        """Delete any local directory made to store the OCN
        """
        if Path(self.dir).exists():
            Path(self.dir).unlink()

    def grd_db_row(self):
        """Creates a dictionary that aligns with our GRD DB columns
        
        Returns:
            dict -- key for each column in our GRD DB, value from SciHub's content
        """        
        tbl = 'shgrd'
        row = {}
        if self.grd: # SciHub has additional information
            row.update({
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
                "identifier" : f"'{self.grd_id}'",
                "uuid" : f"'{self.grd_shid}'", # Foreign Key
                "ocn_uuid" : f"'{self.ocn_shid}'" if self.ocn else 'null', # Foreign Key
            })
        return (row, tbl)

    def ocn_db_row(self):
        """Creates a dictionary that aligns with our OCN DB columns
        
        Returns:
            dict -- key for each column in our OCN DB, value from SciHub's content
        """        
        tbl = 'shocn'
        row = {}
        if self.ocn:
            row.update({
                "uuid" : f"'{self.ocn_shid}'",
                "identifier" : f"'{self.ocn_id}'",
                "summary" : f"'{self.ocn.get('summary')}'",
                "producttype" : f"'{xml_get(self.ocn.get('str'), 'producttype')}'",
                "filename" : f"'{xml_get(self.ocn.get('str'), 'filename')}'",
                "size" : f"'{xml_get(self.ocn.get('str'), 'size')}'",
                "grd_uuid" : f"'{self.grd_shid}'", # Foreign Key
            })
        return (row, tbl)

def xml_get(lst, a, key1="@name", key2="#text"):
    """Extract a field from parsed XML
    
    Arguments:
        lst {list} -- a list of elements all sharing the same data type (e.g. str)
        a {str} -- the name of an XML tag you want
    
    Keyword Arguments:
        key1 {str} -- the field where a is stored (default: {"@name"})
        key2 {str} -- the type of data that a is (default: {"#text"})
    
    Returns:
        any -- the value of the XML tag that has the name 'a'
    """    
    # from a lst of dcts, find the dct that has key value pair (@name:a), then retrieve the value of (#text:?)
    if lst == None: return None # This is a hack for the case where there is no OCN product. TODO handle absent OCN higher up
    for dct in lst:
        if dct.get(key1) == a:
            return dct.get(key2)
    return None

def str_to_ts(s):
    """Turns a string into a timestamp
    
    Arguments:
        s {str} -- one of three formatted strings that occur in SNS or SciHub
    
    Returns:
        timestamp -- seconds since epoch
    """    
    if 'Z' in s:
        if '.' in s:
            fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
        else:
            fmt = '%Y-%m-%dT%H:%M:%SZ'
    else:
        fmt = '%Y-%m-%dT%H:%M:%S'
    return datetime.strptime(s, fmt).timestamp()