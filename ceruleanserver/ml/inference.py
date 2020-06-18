from pathlib import Path
from fastai2.learner import load_learner
from subprocess import run, PIPE
import sys
import json
from ml.vector_processing import unify, sparsify, intersection
from ml.raster_processing import (
    inference_to_poly,
    merge_chips,
    inference_to_geotiff,
    img_chip_generator,
    resize,
)

sys.path.append(str(Path(__file__).parent.parent))
from configs import (  # pylint: disable=import-error
    server_config,
    path_config,
    ml_config,
)
from utils.common import clear, create_pg_array_string


class INFERO:
    """A Class that organizes information about an Inference Object
    """

    def __init__(
        self,
        snso,
        pkls=ml_config.ML_PKL_LIST,
        thresholds=ml_config.ML_THRESHOLDS,
        fine_pkl_idx=-1,
        chip_size_orig=ml_config.CHIP_SIZE_ORIG,
        chip_size_reduced=ml_config.CHIP_SIZE_REDUCED,
        overhang=ml_config.OVERHANG,
        geom_path=None,
    ):
        self.snso = snso
        self.grd_path = snso.grd_path
        self.prod_id = snso.prod_id
        self.pkls = pkls
        self.thresholds = thresholds
        self.fine_pkl_idx = fine_pkl_idx
        self.chip_size_orig = chip_size_orig
        self.chip_size_reduced = chip_size_reduced
        self.overhang = overhang

        self.geom_path = geom_path or self.grd_path.with_name(
            f"slick_{'-'.join([str(t) for t in self.thresholds])}conf.geojson"
        )
        self.has_geometry = None

    def __repr__(self):
        return f"<INFERObject: {self.prod_id or self.grd_path.name}>"

    def run_inference(self):
        multi_machine(self)
        with open(self.geom_path) as f:
            self.geom = json.load(f)
        self.geom["crs"]["properties"][
            "name"
        ] = "urn:ogc:def:crs:EPSG:8.8.1:4326"  # This is equivalent to the existing projectionn, but is recognized by postgres as mappable, so slightly preferred.
        self.geom["features"][0]["geometry"]["crs"] = self.geom[
            "crs"
        ]  # Copy the projection into the multipolygon geometry so that the database doesn't lose the geographic context
        self.has_geometry = any(self.geom["features"][0]["geometry"]["coordinates"])
        return self.geom_path

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

    def inf_db_row(self):
        """Creates a dictionary that aligns with our inference DB columns
        
        Returns:
            dict -- key for each column in our inference DB, value from this SNS's content
        """
        tbl = "inference"
        row = {
            "sns_messageid": f"'{self.snso.sns['MessageId']}'",
            "geometry": f"ST_GeomFromGeoJSON('{json.dumps(self.geom['features'][0]['geometry'])}')",
            "pkls": f"'{create_pg_array_string(self.pkls)}'",
            "thresholds": f"'{create_pg_array_string(self.thresholds)}'",
            "fine_pkl_idx": f"{self.fine_pkl_idx}",
            "chip_size_orig": f"{self.chip_size_orig}",
            "chip_size_reduced": f"{self.chip_size_reduced}",
            "overhang": f"{self.overhang}",
        }
        return (row, tbl)


def multi_machine(infero, out_path=None):
    """Run inference on a GRD using multiple ML PKLs, and combine them to get a single multipolygon

    Arguments:
        pid {str} -- Sentinel 1 product ID, e.g. 'S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112'
        pkls {list of strs} -- list of model pkls to run on indicated product e.g. ["2_18_128_0.676.pkl", "2_18_256_0.691.pkl", "2_18_512_0.705.pkl"]

    Keyword Arguments:
        thresholds {int or list of ints} -- Thresholds to be applied to each ML PKL respectively (default: {ml_config.ML_THRESHOLDS})
        grd_path {Path} -- Location of GRD tiff  (default: {grd_path = Path(path_config.LOCAL_DIR) / "temp" / pid / "vv_grd.tiff"})
        out_path {Path} -- Location where final GeoJSON should be saved (default: {grd_path.with_name(f"slick_{'-'.join([str(t) for t in thresholds])}conf.geojson")})
        fine_pkl_idx {int} -- Which PKL gives the finest resolution result, will be used to trim output (default: {-1})

    Returns:
        Path -- out_path
    """
    path = infero.grd_path.parent

    if not isinstance(infero.thresholds, list):
        infero.thresholds = [infero.thresholds] * len(infero.pkls)

    out_path = out_path or infero.geom_path

    #  Run machine learning on vv_grd to make 3 contours
    geojson_paths = []
    for i, pkl in enumerate(infero.pkls):
        if server_config.VERBOSE:
            print("Running Inference on", pkl)
        inference_path = (path / pkl).with_suffix(".tiff")

        if not inference_path.exists() and server_config.RUN_ML:
            learner = load_learner_from_s3(pkl, False)
            machine(learner, infero, out_path=inference_path)

        geojson_path = inference_to_poly(inference_path, infero.thresholds[i])
        geojson_paths += [geojson_path]

    union_path = unify(geojson_paths)
    coarse_path = sparsify(union_path, geojson_paths)
    out_path = intersection(geojson_paths[infero.fine_pkl_idx], coarse_path, out_path)
    clear(coarse_path)
    return out_path


def machine(learner, infero, out_path=None):
    """Run machine learning on a downloaded image

    Arguments:
        learner {fastai2 learner} -- A trained and loaded learner
        img_path {Path} -- Location of the large image to be processed

    Keyword Arguments:
        inf_dir {Path} -- Directory where inference chips should be stored (default: {None})
        inf_path {Path} -- Location of the stitched-together output (default: {None})
    """
    # Prepare some constants
    img_path = infero.grd_path
    out_path = (
        out_path or img_path.parent / "inference.tiff"
    )  # Where the merged inference mask should be stored
    inf_dir = out_path.parent / "inf"  # Where the inference chips should be stored

    # Cut up the GTiff into many small TIFs
    chp_gen = img_chip_generator(
        infero.grd_path,
        infero.chip_size_orig,
        infero.chip_size_reduced,
        infero.overhang,
        out_dir=inf_dir,
    )
    for i, chp_path in enumerate(chp_gen):
        infer(chp_path, learner)  # Run Inference on the current chip

    # Merge the masks back into a single image
    merge_chips(inf_dir, out_path)


def infer(chip_path, learner):
    """Run inference on a chip

    Arguments:
        chip_path {Path} -- Location of a single chip
        learner {fastai2 model} -- Loaded fastai2 pkl file
    """
    _, _, pred_class = learner.predict(
        chip_path
    )  # Currently returns classes [not bilge, bilge, vessel]
    # target_size = learner.dls.after_batch.size
    target_size = (
        ml_config.CHIP_SIZE_REDUCED
    )  # XXXJona figure out if there is another way to make this work for all learners
    tiff_path = inference_to_geotiff(pred_class[1], chip_path, target_size)
    return tiff_path


def load_learner_from_s3(pkl_name, update_ml=server_config.UPDATE_ML):
    """Import the latest trained model from S3

    Keyword Arguments:
        pkl_name {str} -- Name of pickled model to use (default: {'0_18_512_0.722.pkl'})

    Returns:
        fastai2_learner -- A learner with the model already loaded into memory
    """
    pkl_path = Path(path_config.LOCAL_DIR) / "models" / pkl_name
    if server_config.VERBOSE:
        print("Loading Learner")
    if update_ml:
        clear(pkl_path)
    if not pkl_path.exists():  # pylint: disable=no-member
        src_path = "s3://skytruth-cerulean/model_artifacts/" + str(pkl_name)
        download_str = f"aws s3 cp {src_path} {pkl_path}"
        # print(download_str)
        run(download_str, shell=True)
    return load_learner(pkl_path)


sys.modules["__main__"].__dict__[
    "get_lbls"
] = None  # This is required to enable the pickle to load
