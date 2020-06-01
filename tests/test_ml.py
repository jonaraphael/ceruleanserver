""" Testing the functionality of our data objects """
import pytest
import sys
import json
import shutil
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver"))
from ml import inference  # pylint: disable=import-error
from configs import testing_config, path_config  # pylint: disable=import-error
from utils.common import clear


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
    def get_lbls():  # pylint: disable=unused-variable
        return

    learner = inference.load_learner_from_s3(FILE_ml_pkl)
    assert learner == ""
