""" 
S3 utility functions. 
"""
import boto3
from pathlib import Path
import os
from typing import Union
import sys

sys.path.append(str(Path(__file__).parent.parent))
from configs import aws_config, server_config


def get_s3_bucket(
    bucket_name,
    aws_access_key_id: str = aws_config.ACCESS_KEY,
    aws_secret_access_key: str = aws_config.SECRET_KEY,
):
    session = boto3.Session(aws_access_key_id, aws_secret_access_key)
    s3_resource = session.resource("s3")
    return s3_resource.Bucket(aws_config.S3_BUCKET_NAME)


def download_prefix(
    bucket,
    dest_dir: Union[Path, str],
    s3_prefix: str,
    suffix: str = None,
    recursive=True,
):
    dest_dir = Path(dest_dir)
    objects = bucket.objects
    filter_prefixes = {s3_prefix.lower()}
    while filter_prefixes:
        curr_prefix = filter_prefixes.pop()
        filtered = objects.filter(Prefix=curr_prefix)
        for object in filtered:
            if object.key[-1:] in [
                "/",
                "\\",
            ]:
                if recursive and object.key.lower() != s3_prefix.lower():
                    new_dir_path = object.key[len(s3_prefix) :]
                    full_path = dest_dir / new_dir_path
                    if not full_path.exists():
                        os.mkdir(str(full_path))
                        filter_prefixes.add(object.key)
            elif (
                suffix is None or object.key[-(len(suffix)) :].lower() == suffix.lower()
            ):
                s3_path, filename = os.path.split(object.key)
                if len(s3_path) > len(s3_prefix):
                    fpath = dest_dir / s3_path[len(s3_prefix) :] / filename
                else:
                    fpath = dest_dir / filename
                if server_config.VERBOSE:
                    print(f"downloading {fpath}")
                bucket.download_file(object.key, str(fpath))
