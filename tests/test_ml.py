""" Testing the functionality of our data objects """
import pytest
import sys
import os
import json
import shutil
from pathlib import Path

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(f"{dir_path}/../ceruleanserver")
import ml


@pytest.fixture
def FILE_ml_pkl():
    perm_pkl = Path("tests/test_files_perm/models/ml.pkl")
    temp_pkl = Path("tests/test_files_temp/models/ml.pkl")
    if temp_pkl.exists():
        temp_pkl.unlink()
    temp_pkl.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(perm_pkl), str(temp_pkl))
    yield temp_pkl
    if temp_pkl.exists():
        temp_pkl.unlink()


def test_load_learner(FILE_ml_pkl):
    def get_lbls():
        return

    learner = ml.load_learner_from_s3(FILE_ml_pkl)
    assert learner == ""
