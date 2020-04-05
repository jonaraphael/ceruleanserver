# %%
import os
import xmltodict
from datetime import datetime

class Sho:
    def __init__(self, grd_id, user="jonaraph", pwd="fjjEwvMDHyJH9Fa"):
        self.grd_id = grd_id
        self.user = user
        self.pwd = pwd
        self.dir = grd_id
        self.isoceanic = None
        self.oceanintersection = None

        self.generic_id = grd_id[:7]+"????_?"+grd_id[13:-4]+"*"
        self.URLs = {"query_prods" : f"https://scihub.copernicus.eu/apihub/search?rows=100&q=(platformname:Sentinel-1 AND filename:{self.generic_id})",}
        self.wgets = {"query_prods" : f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document={self.dir}/query_prods_results.xml "{self.URLs["query_prods"]}"',}
        
        os.system(f'mkdir {self.dir}')
        os.system(self.wgets['query_prods'])
        with open(f'{self.dir}/query_prods_results.xml') as f:
            self.query_prods_res = xmltodict.parse(f.read())
        if self.query_prods_res.get('feed').get('opensearch:totalResults') == '0':
            print(f'ERROR No results found matching {self.grd_id}')
            return

        grd = [e for e in self.query_prods_res.get('feed').get('entry') if 'GRD' in e.get('title')][0]
        ocn = [e for e in self.query_prods_res.get('feed').get('entry') if 'OCN' in e.get('title')][0]
        self.ocn_id = ocn.get('title')
        self.ocn_shid = ocn.get('id')
        self.grd_shid = grd.get('id')

        self.URLs['query_grd_tiffs'] = f"https://scihub.copernicus.eu/dhus/odata/v1/Products(\'{self.grd_shid}\')/Nodes(\'{self.grd_id}.SAFE\')/Nodes('measurement')/Nodes"
        self.URLs["download_grd"] = f"https://scihub.copernicus.eu/dhus/odata/v1/Products('{self.grd_shid}')/%24value"
        self.URLs["download_ocn"] = f"https://scihub.copernicus.eu/dhus/odata/v1/Products('{self.ocn_shid}')/%24value"

        self.wgets["query_grd_tiffs"] = f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document={self.dir}/query_grd_tiffs_results.xml "{self.URLs["query_grd_tiffs"]}"'
        self.wgets["download_grd"] = f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document={self.dir}/grd.zip "{self.URLs["download_grd"]}"'
        self.wgets["download_ocn"] = f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document={self.dir}/ocn.zip "{self.URLs["download_ocn"]}"'

        os.system(self.wgets['query_grd_tiffs'])
        with open(f'{self.dir}/query_grd_tiffs_results.xml') as f:
            self.query_grd_tiffs_res = xmltodict.parse(f.read())
        
        grd_vv = [e for e in self.query_grd_tiffs_res.get('feed').get('entry') if '-vv-' in e.get('title').get('#text')][0]
        self.grd_vv = grd_vv.get('title').get('#text')

        self.URLs['download_grd_tiff'] = f"https://scihub.copernicus.eu/dhus/odata/v1/Products('{self.grd_shid}')/Nodes('{self.grd_id}.SAFE')/Nodes('measurement')/Nodes('{self.grd_vv}')/$value"
        self.wgets['download_grd_tiff'] = f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document={self.dir}/grd_vv.tiff "{self.URLs["download_grd_tiff"]}"'

    def __repr__(self):
        return f"<SciHubObject: {self.grd_id}>"

    def download_grd(self): return os.system(self.wgets["download_grd"])
    def download_ocn(self): return os.system(self.wgets["download_ocn"])
    def download_grd_tiff(self): return os.system(self.wgets["download_grd_tiff"])

    def grd_db_row(self):
        grd = [e for e in self.query_prods_res.get('feed').get('entry') if 'GRD' in e.get('title')][0]
        res = {
            "GRD_scihub_uuid" : self.grd_shid,
            "GRD_scihub_identifier" : self.grd_id,
            "summary" : grd.get('summary'),
            "beginposition" : str_to_dt(xml_get(grd.get('date'),'beginposition')),
            "endposition" : str_to_dt(xml_get(grd.get('date'), 'endposition')),
            "ingestiondate" : str_to_dt(xml_get(grd.get('date'), 'ingestiondate')),
            "missiondatatakeid" : int(xml_get(grd.get('int'), 'missiondatatakeid')),
            "orbitnumber" : int(xml_get(grd.get('int'), 'orbitnumber')),
            "lastorbitnumber" : int(xml_get(grd.get('int'), 'lastorbitnumber')),
            "relativeorbitnumber" : int(xml_get(grd.get('int'), 'relativeorbitnumber')),
            "lastrelativeorbitnumber" : int(xml_get(grd.get('int'), 'lastrelativeorbitnumber')),
            "sensoroperationalmode" : xml_get(grd.get('str'), 'sensoroperationalmode'),
            "swathidentifier" : xml_get(grd.get('str'), 'swathidentifier'),
            "orbitdirection" : xml_get(grd.get('str'), 'orbitdirection'),
            "producttype" : xml_get(grd.get('str'), 'producttype'),
            "timeliness" : xml_get(grd.get('str'), 'timeliness'),
            "platformname" : xml_get(grd.get('str'), 'platformname'),
            "platformidentifier" : xml_get(grd.get('str'), 'platformidentifier'),
            "instrumentname" : xml_get(grd.get('str'), 'instrumentname'),
            "instrumentshortname" : xml_get(grd.get('str'), 'instrumentshortname'),
            "filename" : xml_get(grd.get('str'), 'filename'),
            "format" : xml_get(grd.get('str'), 'format'),
            "productclass" : xml_get(grd.get('str'), 'productclass'),
            "polarisationmode" : xml_get(grd.get('str'), 'polarisationmode'),
            "acquisitiontype" : xml_get(grd.get('str'), 'acquisitiontype'),
            "status" : xml_get(grd.get('str'), 'status'),
            "size" : xml_get(grd.get('str'), 'size'),
            "footprint" : xml_get(grd.get('str'), 'footprint'),
            "isoceanic" : self.isoceanic,
            "oceanintersection" : self.oceanintersection, 
            "timestamp" : str_to_dt(xml_get(grd.get('date'),'beginposition')).timestamp(),
        }
        return res

    def ocn_db_row(self):
        ocn = [e for e in self.query_prods_res.get('feed').get('entry') if 'OCN' in e.get('title')][0]
        res = {
            "OCN_scihub_uuid" : self.ocn_shid,
            "OCN_scihub_identifier" : self.ocn_id,
            "producttype" : xml_get(ocn.get('str'), 'producttype'),
            "filename" : xml_get(ocn.get('str'), 'filename'),
            "size" : xml_get(ocn.get('str'), 'size'),
            "GRD_scihub_uuid" : self.grd_shid,
        }
        return res
    
    def cleanup(self):
        os.system(f'rm -f -r {self.dir}')
        return True

def xml_get(lst, a, key1="@name", key2="#text"):
    # from a lst of dcts, find the dct that has key value pair (@name:a), then retrieve the value of (#text:?)
    for dct in lst:
        if dct.get(key1) == a:
            return dct.get(key2)
    return None

def str_to_dt(s): return datetime.strptime(s[:-1], '%Y-%m-%dT%H:%M:%S.%f')

a = Sho("S1B_IW_GRDH_1SDV_20200402T003114_20200402T003139_020958_027C13_47BB")
a
# %%
a.download_grd_tiff()

# %%
