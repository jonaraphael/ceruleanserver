from typing import Union
import os
from pathlib import Path
import os
import sys

sys.path.append(Path(__file__).parent.parent)
from configs import aws_config


import aws_config

datapath = Path(__file__).parent / "data"

if not datapath.exists():
    os.mkdir(datapath)

if not any(x for x in datapath.iterdir()):
    bucket = get_s3_bucket()
    dir_prefix_map = [("ocn", aws_config.OCN_PREFIX)]
    for dirname, s3_prefix in dir_prefix_map:
        os.mkdir(datapath / dirname)
        download_data(bucket, datapath / dirname, s3_prefix)
