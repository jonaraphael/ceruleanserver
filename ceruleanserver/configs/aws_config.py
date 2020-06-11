ACCESS_KEY = "placeholder"
SECRET_KEY = "placeholder"

S3_BUCKET_NAME = "skytruth-cerulean"
S3_SENTINEL_CHIPS_PATH = "labeled_data/chips"
S3_OCN_CHIPS_PATH = "labeled_data/ocn"
S3_MODELS_PATH = "model_artifacts"
S3_MODEL_NAME = "18_512_0.722.pkl"
S3_MODEL_FULL_PATH = f"s3://{S3_BUCKET_NAME}/{S3_MODELS_PATH}/{S3_MODEL_NAME}"

# Import credentials from gitignored file
import importlib
if importlib.util.find_spec("configs.aws_credentials") is not None:
    from configs.aws_credentials import *  # pylint: disable=unused-wildcard-import
