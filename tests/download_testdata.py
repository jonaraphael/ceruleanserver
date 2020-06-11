""" This is a model training script for ad-hoc, manual usage.
"""
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver"))
from utils import s3  # pylint: disable=import-error
from configs import (  # pylint: disable=import-error
    testing_config,
    aws_config,
    path_config,
)

version_dir = (
    Path(path_config.LOCAL_DIR)
    / testing_config.TESTDATA_FOLDER
    / testing_config.TESTDATA_VERSION
)
if version_dir.exists():
    print(f"Download target directory {version_dir} already exists. Skipping download.")
else:
    version_dir.mkdir(parents=True)
    bucket = s3.get_s3_bucket(aws_config.S3_BUCKET_NAME)
    s3_prefix = f"{testing_config.TESTDATA_FOLDER}/{testing_config.TESTDATA_VERSION}/"
    s3.download_prefix(bucket, version_dir, s3_prefix)
