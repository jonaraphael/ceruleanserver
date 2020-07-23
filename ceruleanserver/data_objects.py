import xmltodict
from configs import server_config, ml_config, path_config
import json
import requests
import shutil
from subprocess import run
from pathlib import Path
from errors import MissingProductError
from utils.common import xml_get, clear
from utils.s3 import s3copy
from alchemy import Sns, Grd, Ocn, Inference, Posi_Poly
from ml.raster_processing import resize
from ml.vector_processing import geojson_to_ewkt, shape_to_ewkt


class Sns_:
    """A Class that extends the Sns class and organizes information about the SNS
    """

    def __init__(self, raw):
        # DB Columns
        self.messageid = raw["MessageId"]
        self.subject = raw["Subject"]
        self.timestamp = raw["Timestamp"]
        self.db = Sns(
            messageid=self.messageid, subject=self.subject, timestamp=self.timestamp
        )

        # Calculated
        self.raw = raw
        self.message = json.loads(raw["Message"])


class Grd_:
    def __init__(self, sns):
        # DB Columns
        self.sns = sns
        self.pid = sns.message["id"]
        self.uuid = sns.message["sciHubId"]
        self.absoluteorbitnumber = sns.message["absoluteOrbitNumber"]
        self.polarization = sns.message["polarization"]
        self.mode = sns.message["mode"]
        self.s3ingestion = sns.message["s3Ingestion"]
        self.scihubingestion = sns.message["sciHubIngestion"]
        self.starttime = sns.message["startTime"]
        self.stoptime = sns.message["stopTime"]
        self.geometry = geojson_to_ewkt(sns.message["footprint"])
        self.db = Grd(
            sns=self.sns.db,
            pid=self.pid,
            uuid=self.uuid,
            absoluteorbitnumber=self.absoluteorbitnumber,
            mode=self.mode,
            polarization=self.polarization,
            s3ingestion=self.s3ingestion,
            scihubingestion=self.scihubingestion,
            starttime=self.starttime,
            stoptime=self.stoptime,
            geometry=self.geometry,
        )

        # Calculated
        self.sns_msg = sns.message
        self.s3_dir = f"s3://sentinel-s1-l1c/{self.sns_msg['path']}/measurement/"

        # Placeholders
        self.filepath = None
        self.is_downloaded = False

    def download_grd_tiff(self, dest_dir=None):
        """Creates a local directory and downloads a GeoTiff (often ~700MB)
        """
        if server_config.VERBOSE:
            print("Downloading GRD")

        dest_dir = (
            Path(dest_dir)
            if dest_dir
            else Path(path_config.LOCAL_DIR) / "temp" / self.pid
        )
        self.file_path = dest_dir / f"{self.sns_msg['mode'].lower()}-vv.tiff"
        s3copy(self.file_path.name, self.s3_dir, dest_dir)
        self.is_downloaded = True
        return self.file_path

    def cleanup(self):
        """Delete any local directory made to store the GRD
        """
        if self.file_path.parent.exists():
            shutil.rmtree(self.file_path.parent)


class Ocn_:
    def __init__(self, grd, ocn_xml):
        # DB Columns
        self.grd = grd
        self.pid = ocn_xml.get("title")
        self.uuid = ocn_xml.get("id")
        self.summary = ocn_xml.get("summary")
        self.producttype = xml_get(ocn_xml.get("str"), "producttype")
        self.filename = xml_get(ocn_xml.get("str"), "filename")

        self.db = Ocn(
            grd=self.grd.db,
            pid=self.pid,
            uuid=self.uuid,
            summary=self.summary,
            producttype=self.producttype,
            filename=self.filename,
        )

        # Calculated
        self.file_path = None


class Inference_:
    """A Class that extends the Inference class and organizes information about the ml outputs
    """

    def __init__(
        self,
        grd,
        ocn,
        ml_pkls=ml_config.ML_PKL_LIST,
        thresholds=ml_config.ML_THRESHOLDS,
        fine_pkl_idx=-1,
        chip_size_orig=ml_config.CHIP_SIZE_ORIG,
        chip_size_reduced=ml_config.CHIP_SIZE_REDUCED,
        overhang=ml_config.OVERHANG,
        use_ocn=ml_config.USE_OCN,
        geom_path=None,
    ):
        # DB Columns
        self.grd = grd
        self.ocn = ocn
        self.ml_pkls = ml_pkls
        self.thresholds = thresholds
        self.fine_pkl_idx = fine_pkl_idx
        self.chip_size_orig = chip_size_orig
        self.chip_size_reduced = chip_size_reduced
        self.overhang = overhang
        self.db = Inference(
            grd=self.grd.db,
            ocn=self.ocn.db if use_ocn else None,
            ml_pkls=self.ml_pkls,
            thresholds=self.thresholds,
            fine_pkl_idx=self.fine_pkl_idx,
            chip_size_orig=self.chip_size_orig,
            chip_size_reduced=self.chip_size_reduced,
            overhang=self.overhang,
        )

        # Calculated
        self.polys = []  # Shapely Objects
        self.grd_path = grd.file_path
        self.prod_id = grd.pid
        self.geom_path = geom_path or self.grd_path.with_name(
            f"slick_{'-'.join([str(t) for t in self.thresholds])}conf.geojson"
        )
        self.use_ocn = use_ocn

    def save_small_to_s3(self, pct=0.25):
        small_path = self.grd_path.with_name("small.tiff")
        resize(self.grd_path, small_path, pct)
        s3_raster_path = f"s3://skytruth-cerulean/outputs/rasters/{self.prod_id}.tiff"
        cmd = f"aws s3 cp {small_path} {s3_raster_path}"
        run(cmd, shell=True)
        clear(small_path)

    def save_poly_to_s3(self):
        s3_vector_path = (
            f"s3://skytruth-cerulean/outputs/vectors/{self.prod_id}.geojson"
        )
        cmd = f"aws s3 cp {self.geom_path} {s3_vector_path}"
        run(cmd, shell=True)


class Posi_Poly_:
    """A Class that extends the Posi Poly class and organizes information about the polygons
    """

    def __init__(self, inf, geoshape):
        # DB Columns
        self.inference = inf
        self.geometry = geoshape
        self.db = Posi_Poly(
            inference=self.inference.db, geometry=shape_to_ewkt(self.geometry)
        )


class SHO:
    """A class that organizes information about content stored on SciHub
    """

    def __init__(self, grd, user=server_config.SH_USER, pwd=server_config.SH_PWD):
        self.prod_id = grd.pid
        self.generic_id = self.prod_id[:7] + "????_?" + self.prod_id[13:-4] + "*"
        self.URLs = {
            "query_prods": f"https://{user}:{pwd}@scihub.copernicus.eu/apihub/search?q=(platformname:Sentinel-1 AND filename:{self.generic_id})",
        }

        # Placeholders
        self.grd_xml = self.grd_id = self.grd_shid = self.grd_path = None
        self.ocn = self.ocn_xml = None

        with requests.Session() as s:
            try:
                p = s.post(self.URLs["query_prods"])
            except requests.exceptions.ConnectionError as e:
                print("Error connecting to SciHub")
                raise e
            self.query_prods_res = xmltodict.parse(p.text)

        if self.query_prods_res.get("feed").get("opensearch:totalResults") != "0":
            prods = self.query_prods_res.get("feed").get("entry")
            if isinstance(prods, dict):
                prods = [
                    prods
                ]  # If there's only one product, xmlparser returns a dict instead of a list of dicts
            for p in prods:
                self.grd_xml = (
                    p if "GRD" in p.get("title") else self.grd_xml
                )  # This is XML
                self.ocn_xml = (
                    p if "OCN" in p.get("title") else self.ocn_xml
                )  # This is XML

            if self.grd_xml:
                self.grd_id = self.grd_xml.get("title")
                self.grd_shid = self.grd_xml.get("id")
                self.is_vv = "VV" in xml_get(
                    self.grd_xml.get("str"), "polarisationmode"
                )

            if self.ocn_xml:
                self.ocn = Ocn_(grd, self.ocn_xml)
                self.URLs[
                    "download_ocn"
                ] = f"https://{user}:{pwd}@scihub.copernicus.eu/dhus/odata/v1/Products('{self.ocn.uuid}')/%24value"

        else:
            pass  # There are no products listed! https://app.asana.com/0/1170930608885369/1171069537674895

    def __repr__(self):
        return f"<SciHubObject: {self.prod_id}>"

    def download_ocn(self, ocn_path=None):
        """Create a local directory, and download an OCN zip file to it
        """
        if server_config.VERBOSE:
            print("Downloading OCN")
        if not self.ocn_xml:
            raise MissingProductError(
                product_id=self.prod_id, message="ERROR: No OCN found for this GRD"
            )

        self.ocn.file_path = (
            ocn_path or Path(path_config.LOCAL_DIR) / "temp" / self.ocn.pid / "ocn.zip"
        )
        self.ocn.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.ocn.file_path.exists():
            with requests.Session() as s:
                try:
                    p = s.get(self.URLs.get("download_ocn"))
                except requests.exceptions.ConnectionError as e:
                    print("Error connecting to SciHub")
                    raise e
                open(self.ocn.file_path, "wb").write(p.content)
        return self.ocn.file_path

    def cleanup(self, grd=True, ocn=True):
        """Delete any local directory made to store the GRD and OCN
        """
        if self.grd_path.parent.exists() and grd:
            shutil.rmtree(self.grd_path.parent)
        if self.ocn.file_path.parent.exists() and ocn:
            shutil.rmtree(self.ocn.file_path.parent)

