from typing import Union
import os
from pathlib import Path
import os
import sys

sys.path.append(str(Path(__file__).parent.parent / 'ceruleanserver' ))
from configs import aws_config
from utils import s3

datapath = Path(__file__).parent / "data"

if not datapath.exists():
    os.mkdir(datapath)

if not any(x for x in datapath.iterdir()):
    bucket = s3.get_s3_bucket(aws_config.S3_BUCKET_NAME)
    s3.download_prefix(bucket, datapath, aws_config.S3_TRAINING_DATA_PATH, recursive=True)
