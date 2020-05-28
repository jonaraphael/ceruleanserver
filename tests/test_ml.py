""" Testing the functionality of our data objects """
import pytest
import sys
import os
import json
import shutil
from pathlib import Path

thisfile_dir = (Path(__file__)).parent
sys.path.append(str(thisfile_dir.parent / 'ceruleanserver'))
import ml
import configs # to fix pytest import problems
from configs import testing_config

def test_load_learner():
    def get_lbls():
        return

    learner = ml.load_learner_from_s3(thisfile_dir / testing_config.TEMP_FILES_PATH)
    assert learner == ""
