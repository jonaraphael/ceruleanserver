from pathlib import Path
from fastai2.learner import load_learner
from subprocess import run, PIPE
import sys
import json
from ml.vector_processing import (
    unify,
    sparsify,
    intersection,
    shapely_to_geojson,
)
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


def run_inference(infero):
    multi_machine(infero)
    return infero.geom_path


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
    working_dir = infero.grd_path.parent
    out_path = out_path or infero.geom_path

    if not isinstance(infero.thresholds, list):
        infero.thresholds = [infero.thresholds] * len(infero.ml_pkls)

    #  Run machine learning on vv_grd to make 3 contours
    geojson_paths = []
    for i, pkl in enumerate(infero.ml_pkls):
        if server_config.VERBOSE:
            print("Running Inference on", pkl)
        inference_path = (working_dir / pkl).with_suffix(".tiff")

        if not inference_path.exists() and server_config.RUN_ML:
            learner = load_learner_from_s3(pkl, False)
            machine(learner, infero, out_path=inference_path)

        geojson_path = inference_to_poly(inference_path, infero.thresholds[i])
        geojson_paths += [geojson_path]

    union = unify(geojson_paths) # Returnds shapely multipolygon
    sparse = sparsify(union, geojson_paths)
    inter = intersection(geojson_paths[infero.fine_pkl_idx], sparse)
    infero.polys = [poly for poly in inter]
    shapely_to_geojson(inter, out_path)
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
