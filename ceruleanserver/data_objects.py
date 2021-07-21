import xmltodict
from configs import server_config, ml_config, path_config
import json
import requests
import shutil
from subprocess import run
from pathlib import Path
from errors import MissingProductError
from utils.common import (  # pylint: disable=no-name-in-module
    xml_get,
    clear,
    to_standard_datetime_str,
)
from utils.s3 import s3copy  # pylint: disable=no-name-in-module
from alchemy import (
    Sns,
    Grd,
    Ocn,
    Inference,
    Posi_Poly,
    Vessel,
    Coincident,
    Slick,
    Eez,
)
from sqlalchemy.orm import relationship, reconstructor
from ml.raster_processing import resize
from ml.vector_processing import geojson_to_ewkt, shape_to_ewkt
import shapely.geometry as sh
from geoalchemy2.shape import to_shape
from geoalchemy2.elements import WKTElement, WKBElement


class Sns_Ext(Sns):
    # Database Relationships
    grd = relationship(
        "Grd_Ext",
        back_populates="sns",
        uselist=False,
        enable_typechecks=False,  # XXXHELP How can I do this without disabling typechecks?
        cascade_backrefs=False,
    )

    def __init__(self, raw):
        # DB Columns
        self.messageid = raw["MessageId"]
        self.subject = raw["Subject"]
        self.timestamp = raw["Timestamp"]

        # Calculated
        self.raw = raw
        self.message = json.loads(raw["Message"])


class Grd_Ext(Grd):
    # Default values
    file_path = None
    s3_dir = None
    
    # Database Relationships
    sns = relationship(
        "Sns_Ext",
        back_populates="grd",
        foreign_keys=Grd.sns__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    ocn = relationship(
        "Ocn_Ext",
        back_populates="grd",
        uselist=False,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    inferences = relationship(
        "Inference_Ext",
        back_populates="grd",
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    
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

        # Calculated
        self.s3_dir = f"s3://sentinel-s1-l1c/{sns.message['path']}/measurement/"

    def download_grd_tiff(self, dest_dir=None):
        """Creates a local directory and downloads a GeoTiff (often ~700MB)
        """
        if self.loaded_from_db: # This Grd was copied in from the DB, and does not have the a record of the s3 directory
            raise Exception(f"Load Error: {self} was loaded from the DB and is missing a critical piece of information not stored there.")

        if server_config.VERBOSE:
            print("Downloading GRD")

        dest_dir = (
            Path(dest_dir)
            if dest_dir
            else Path(path_config.LOCAL_DIR) / "temp" / self.pid
        )
        self.file_path = dest_dir / f"{self.mode.lower()}-vv.tiff"
        s3copy(self.file_path.name, self.s3_dir, dest_dir)
        self.is_downloaded = True
        return self.file_path

    def cleanup(self):
        """Delete any local directory made to store the GRD
        """
        if self.file_path and self.file_path.parent.exists():
            shutil.rmtree(self.file_path.parent)
        else:
            print("WARNING: File path not found when attempting to delete.")


class Ocn_Ext(Ocn):
    # Default values
    file_path = None

    # Database Relationships
    grd = relationship(
        "Grd_Ext",
        back_populates="ocn",
        foreign_keys=Ocn.grd__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    inferences = relationship(
        "Inference_Ext",
        back_populates="ocn",
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    
    def __init__(self, grd, ocn_xml):
        # DB Columns
        self.grd = grd
        self.pid = ocn_xml.get("title")
        self.uuid = ocn_xml.get("id")
        self.summary = ocn_xml.get("summary")
        self.producttype = xml_get(ocn_xml.get("str"), "producttype")
        self.filename = xml_get(ocn_xml.get("str"), "filename")


class Inference_Ext(Inference):
    # Default values
    polys = []  # Shapely Objects
    posi_polys = []  # SQLAlchemy objects

    # Database Relationships
    grd = relationship(
        "Grd_Ext",
        back_populates="inferences",
        foreign_keys=Inference.grd__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    ocn = relationship(
        "Ocn_Ext",
        back_populates="inferences",
        foreign_keys=Inference.ocn__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    posi_polys = relationship(
        "Posi_Poly_Ext",
        back_populates="inference",
        enable_typechecks=False,
        cascade_backrefs=False,
    )

    def __init__(
        self,
        grd,
        ocn=None,
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

        # Calculated
        self.grd_path = grd.file_path
        self.prod_id = grd.pid
        self.geom_path = geom_path or self.grd_path.with_name(
            f"slick_{'-'.join([str(t) for t in self.thresholds])}conf.geojson"
        )

    def save_small_to_s3(self, pct=0.25):
        if self.loaded_from_db: # This object was copied in from the DB, and does not have the a record of grd_path
            raise Exception(f"Load Error: {self} was loaded from the DB and is missing a critical piece of information not stored there.")
            
        small_path = self.grd_path.with_name("small.tiff")
        resize(self.grd_path, small_path, pct)
        s3_raster_path = f"s3://skytruth-cerulean/outputs/rasters/{self.prod_id}.tiff"
        cmd = f"aws s3 cp {small_path} {s3_raster_path}"
        run(cmd, shell=True)
        clear(small_path)

    def save_poly_to_s3(self):
        if self.loaded_from_db: # This object was copied in from the DB, and does not have the a record of prod_id
            raise Exception(f"Load Error: {self} was loaded from the DB and is missing a critical piece of information not stored there.")
        s3_vector_path = (
            f"s3://skytruth-cerulean/outputs/vectors/{self.prod_id}.geojson"
        )
        cmd = f"aws s3 cp {self.geom_path} {s3_vector_path}"
        run(cmd, shell=True)


class Posi_Poly_Ext(Posi_Poly):
    # Database Relationships
    inference = relationship(
        "Inference_Ext",
        back_populates="posi_polys",
        foreign_keys=Posi_Poly.inference__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    slick = relationship(
        "Slick_Ext",
        back_populates="posi_polys",
        foreign_keys=Posi_Poly.slick__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    coincidents = relationship(
        "Coincident_Ext",
        back_populates="posi_poly",
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    
    def __init__(self, inf=None, geoshape=None, slick=None):
        # DB Columns
        self.inference = inf
        self.geometry = shape_to_ewkt(geoshape)
        self.slick = slick

    def calc_eezs(self, sess):
        # XXX This set command isn't working.
        eez_ids = set([e.id for e in self.get_intersecting_objects(sess, Eez)])
        return [Eez_Ext.from_id(e_id, sess) for e_id in eez_ids]


class Vessel_Ext(Vessel):
    # Database Relationships
    coincidents = relationship(
        "Coincident_Ext",
        back_populates="vessel",
        enable_typechecks=False,
        cascade_backrefs=False,
    )

    def __init__(self, inf, geoshape):
        # DB Columns
        self.inference = inf
        self.geometry = shape_to_ewkt(geoshape)


class Coincident_Ext(Coincident):
    # Database Relationships
    posi_poly = relationship(
        "Posi_Poly_Ext",
        back_populates="coincidents",
        foreign_keys=Coincident.posi_poly__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    vessel = relationship(
        "Vessel_Ext",
        back_populates="coincidents",
        foreign_keys=Coincident.vessel__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    
    def __init__(self, posi_poly, vessel, input="???"):
        # DB Columns
        self.posi_poly = posi_poly
        self.vessel = vessel

        # Placeholders
        self.direct_hits = None
        self.proximity = None
        self.score = None
        self.method = None
        self.destination = None
        self.speed_avg = None
        self.status = None
        self.port_last = None
        self.port_next = None
        self.cargo_type = None
        self.cargo_amount = None

    def to_api_dict(self):
        res = {
            "id": self.vessel.id,
            "mmsi": self.vessel.mmsi,
            "name": self.vessel.name,
            "flag": self.vessel.flag,
            "callsign": self.vessel.callsign,
            "imo": self.vessel.imo,
            "shiptype": self.vessel.shiptype,
            "length": self.vessel.length,
            "direct_hits": self.direct_hits,
            "proximity": self.proximity,
            "score": self.score,  # ORDER LIST BY THIS VALUE
            "method": self.method,
            "destination": self.destination,
            "speed_avg": self.speed_avg,
        }
        return res


class Slick_Ext(Slick):
    # Database Relationships
    posi_polys = relationship(
        "Posi_Poly_Ext",
        back_populates="slick",
        enable_typechecks=False,
        cascade_backrefs=False,
    )

    def __init__(self, posi_polys=[]):
        # DB Columns
        self.posi_polys = posi_polys

        # Calculated
        self.timestamp = self.calc_timestamp()
        self.geometry = shape_to_ewkt(self.calc_geometry())
        self.coincidents = self.calc_coincidents()
        
    @reconstructor # This code will be run once when this object is loaded from the DB. It cannot accept parameters
    def init_on_load(self):
        self.loaded_from_db = True
        self.timestamp = self.calc_timestamp()
        self.geometry = shape_to_ewkt(self.calc_geometry())
        self.coincidents = self.calc_coincidents()

    def to_api_dict(self, sess):  # XXXHELP Should the session just be a global variable?
        res = {
            "id": self.id,
            "timestamp": to_standard_datetime_str(self.timestamp),
            "geometry": self.geometry,
            "eezs": [eez.to_api_dict() for eez in self.calc_eezs(sess)],
            "coincidents": [
                coincident.to_api_dict() for coincident in self.coincidents
            ],
        }
        return res

    def calc_timestamp(self):
        return self.posi_polys[0].inference.grd.starttime

    def calc_geometry(self):
        if isinstance(self.posi_polys[0].geometry, WKBElement):
            shp_polys = [to_shape(poly.geometry) for poly in self.posi_polys]
        else:
            shp_polys = [to_shape(WKTElement(poly.geometry, extended=True)) for poly in self.posi_polys]
        return sh.MultiPolygon(shp_polys)

    def calc_eezs(self, sess):
        eez_ids = []
        for poly in self.posi_polys:
            eez_ids += [e.id for e in poly.calc_eezs(sess)]
        return [Eez_Ext.from_id(e_id, sess) for e_id in set(eez_ids)]

    def calc_coincidents(self):
        coincidents = []
        for poly in self.posi_polys:
            coincidents += poly.coincidents
        return coincidents


class Eez_Ext(Eez):
    def to_api_dict(self):
        res = {
            "id": self.id,
            "mrgid": self.mrgid,
            "geoname": self.geoname,
            "pol_type": self.pol_type,
            "sovereigns": self.sovereigns,
        }
        return res


class SHO:
    """A class that organizes information about content stored on SciHub
    """
    def __init__(self, grd, sess, user=server_config.SH_USER, pwd=server_config.SH_PWD):
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
                self.ocn = Ocn_Ext(grd, self.ocn_xml)
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

