""" 
S3 utility functions. 
"""
from subprocess import run, Popen
import boto3
from pathlib import Path
import os
from typing import Union
import sys

sys.path.append(str(Path(__file__).parent.parent))
from configs import aws_config, server_config, path_config

def s3sync(
    include_files,
    source_path=f"s3://{aws_config.S3_BUCKET_NAME}/",
    dest_path=f"{path_config.LOCAL_DIR} / temp /",
    exclude_files="*",
):
    if not isinstance(include_files, (list, tuple)):
        include_files = [include_files]
    include_str = " ".join([f'--include "{i}"' for i in include_files])

    if not isinstance(exclude_files, (list, tuple)):
        exclude_files = [exclude_files]
    exclude_str = " ".join([f'--exclude "{e}"' for e in exclude_files])

    sync_str = f"aws s3 sync {source_path} {dest_path} {exclude_str} {include_str}"
    run(sync_str, shell=True)

    return dest_path

def s3copy(include_files,
    source_path=f"s3://{aws_config.S3_BUCKET_NAME}/",
    dest_path=f"{path_config.LOCAL_DIR} / temp /",
    exclude_files="*",
    overwrite=False
):
    if not isinstance(include_files, (list, tuple)):
        include_files = [include_files]
    include_str = " ".join([f'--include "{i}"' for i in include_files])

    if not isinstance(exclude_files, (list, tuple)):
        exclude_files = [exclude_files]
    exclude_str = " ".join([f'--exclude "{e}"' for e in exclude_files])
    
    copy_str = f"aws s3 cp {source_path} {dest_path} --recursive {exclude_str} {include_str} --request-payer"

    if overwrite or not all([(Path(dest_path)/i).exists() for i in include_files]):
        run(copy_str, shell=True)

    return dest_path


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


def sync_grds_and_vecs(pids, separate_process=False):
    cmd = f'aws s3 sync s3://skytruth-cerulean/outputs/ {path_config.LOCAL_DIR}temp/outputs/ --exclude "*" '

    include_tiffs = " ".join([f'--include "rasters/{pid}.tiff" ' for pid in pids])
    cmd = cmd + include_tiffs

    include_geos = " ".join([f'--include "vectors/{pid}.geojson" ' for pid in pids])
    cmd = cmd + include_geos

    if separate_process:
        Popen(cmd, shell=True)
    else:
        run(cmd, shell=True)


