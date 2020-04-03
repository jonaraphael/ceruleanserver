# %%
import os
import xmltodict

class Sho:
    def __init__(self, grd_id, user="jonaraph", pwd="fjjEwvMDHyJH9Fa"):
        self.grd_id = grd_id
        self.user = user
        self.pwd = pwd

        self.generic_id = grd_id[:7]+"????_?"+grd_id[13:-4]+"*"
        self.URLs = {"query_prods" : f"https://scihub.copernicus.eu/apihub/search?rows=100&q=(platformname:Sentinel-1 AND filename:{self.generic_id})",}
        self.wgets = {"query_prods" : f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document=query_prods_results.xml "{self.URLs["query_prods"]}"',}
        
        os.system(self.wgets['query_prods'])
        with open('query_prods_results.xml') as f:
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

        self.wgets["query_grd_tiffs"] = f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document=query_grd_tiffs_results.xml "{self.URLs["query_grd_tiffs"]}"'
        self.wgets["download_grd"] = f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document=grd.zip "{self.URLs["download_grd"]}"'
        self.wgets["download_ocn"] = f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document=ocn.zip "{self.URLs["download_ocn"]}"'

        os.system(self.wgets['query_grd_tiffs'])
        with open('query_grd_tiffs_results.xml') as f:
            self.query_grd_tiffs_res = xmltodict.parse(f.read())
        
        grd_vv = [e for e in self.query_grd_tiffs_res.get('feed').get('entry') if '-vv-' in e.get('title').get('#text')][0]
        self.grd_vv = grd_vv.get('title').get('#text')

        self.URLs['download_grd_tiff'] = f"https://scihub.copernicus.eu/dhus/odata/v1/Products('{self.grd_shid}')/Nodes('{self.grd_id}.SAFE')/Nodes('measurement')/{self.grd_vv}/$value"
        self.wgets['download_grd_tiff'] = f'wget --no-check-certificate --user={self.user} --password={self.pwd} --output-document=grd_vv.tiff "{self.URLs["download_grd_tiff"]}"'

    def __repr__(self):
        return f"<SciHubObject: {self.grd_id}>"

    def download_grd(self): return os.system(self.wgets["download_grd"])
    def download_ocn(self): return os.system(self.wgets["download_ocn"])
    def download_grd_tiff(self): return os.system(self.wgets["download_grd_tiff"])

a = Sho("S1B_IW_GRDH_1SDV_20200402T003114_20200402T003139_020958_027C13_47BB")
a
# %%
