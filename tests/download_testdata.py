""" This is a model training script for ad-hoc, manual usage.
"""
import os
from pathlib import Path
import sys
thisfile_dir = (Path(__file__)).parent
sys.path.append(str(thisfile_dir.parent / 'ceruleanserver'))
import utils.s3 as s3
import configs.testing_config as test_config
import configs.aws_config as aws_config

data_path = thisfile_dir / test_config.LOCAL_TESTDATA_PATH
if not Path(data_path).exists(): os.mkdir(str(data_path))
s3_prefix = test_config.S3_TESTDATA_PATH
version_path = data_path / test_config.S3_TESTDATA_VERSION
if not Path(version_path).exists():
    os.mkdir(str(version_path))
    bucket = s3.get_s3_bucket(aws_config.S3_BUCKET_NAME)
    s3.download_prefix(bucket, version_path, s3_prefix)
else:
    print(f'Download target directory {version_path} already exists. Skipping download.')