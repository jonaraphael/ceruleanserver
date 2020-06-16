""" Testing the functionality of our data objects """
import pytest
import sys
import json
import shutil
from pathlib import Path
from unittest.mock import Mock

sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver"))
sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver" / "ml"))
sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver" / "utils"))
from configs import testing_config, path_config  # pylint: disable=import-error
from common import clear
from inference import load_learner_from_s3, INFERO


@pytest.fixture
def FILE_ml_pkl():
    test_dir = (
        Path(path_config.LOCAL_DIR)
        / testing_config.TESTDATA_FOLDER
        / testing_config.TESTDATA_VERSION
    )
    perm_pkl = test_dir / "perm/models/ml.pkl"
    temp_pkl = test_dir / "temp/models/ml.pkl"
    clear(temp_pkl)
    temp_pkl.parent.mkdir(parents=True, exist_ok=True)  # pylint: disable=no-member
    shutil.copy2(str(perm_pkl), str(temp_pkl))
    yield temp_pkl
    clear(temp_pkl)


def test_load_learner(FILE_ml_pkl):
    sys.modules["__main__"].__dict__[
        "get_lbls"
    ] = None  # This is required to enable the pickle to load

    learner = load_learner_from_s3(FILE_ml_pkl)
    assert learner.model[0][0].kernel_size[0] == 7


#%% Test INFERO


@pytest.fixture
def FILE_grd_path():
    test_dir = (
        Path(path_config.LOCAL_DIR)
        / testing_config.TESTDATA_FOLDER
        / testing_config.TESTDATA_VERSION
    )
    perm_grd = (
        test_dir
        / "perm/S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112/vv_grd.tiff"
    )
    temp_grd = (
        test_dir
        / "temp/S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112/vv_grd.tiff"
    )
    clear(temp_grd)
    temp_grd.parent.mkdir(parents=True, exist_ok=True)  # pylint: disable=no-member
    shutil.copy2(str(perm_grd), str(temp_grd))
    yield temp_grd
    clear(temp_grd)


def test_infero(FILE_grd_path):
    mocksnso = Mock()
    mocksnso.configure_mock(grd_path=FILE_grd_path, prod_id="S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112"})
    infero = INFERO(mocksnso)
    assert infero.chip_size_orig == 4096


@pytest.fixture
def infero(FILE_grd_path):
    mocksnso = Mock()
    mocksnso.configure_mock(grd_path=FILE_grd_path, prod_id="S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112")
    return INFERO(
        mocksnso,
        pkls=["2_18_128_0.676.pkl"],
        thresholds=[128],
    )


def test_run_inference(infero):
    sys.modules["__main__"].__dict__[
        "get_lbls"
    ] = None  # This is required to enable the pickle to load

    infero.run_inference()
    assert infero.thresholds == [128]
    assert infero.geom_path.name == "slick_128conf.geojson"
    assert infero.geom_path.exists()
    assert infero.has_geometry
    row, tbl = infero.inf_db_row()
    assert tbl == "inference"
    assert infero.geom["features"][0]["geometry"]["coordinates"][0][0][0][0] > 140

