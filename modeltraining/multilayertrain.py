""" This ad hoc 
"""
import boto3
import os
import aws_config
from pathlib import Path
import os
from typing import Union


def get_s3_bucket():
    session = boto3.Session(
        aws_access_key_id=aws_config.ACCESS_KEY,
        aws_secret_access_key=aws_config.SECRET_KEY,
    )
    s3_resource = session.resource("s3")
    return s3_resource.Bucket(aws_config.BUCKET_NAME)


def download_data(bucket, target_dir: Union[Path, str], s3_prefix: str=None):
    target_dir = Path(target_dir)
    objects = bucket.objects
    if s3_prefix is not None: 
        objects = objects.filter(Prefix=s3_prefix)
    for object in objects:
        if object.key[-4:] == ".png":
            s3_path, filename = os.path.split(object.key)
            bucket.download_file(object.key, str(target_dir / filename))


datapath = Path(__file__).parent / "data"

if not datapath.exists():
    os.mkdir(datapath)

if not any(x for x in datapath.iterdir()):
    bucket = get_s3_bucket()
    dir_prefix_map = [("ocn", aws_config.OCN_PREFIX)]
    for dirname, s3_prefix in dir_prefix_map:
        os.mkdir(datapath / dirname)
        download_data(bucket, datapath / dirname, s3_prefix)
