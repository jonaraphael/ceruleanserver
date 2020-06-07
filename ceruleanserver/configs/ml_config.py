""" This module stores the default config settings for the server.
It is optionally overriden by a local_config module in the same directory.
The local configs are for dev debugging/testing.
"""
import importlib

# ML Settings
ML_PKL_LIST = ["2_18_128_0.676.pkl"]#, "2_18_256_0.691.pkl", "2_18_512_0.705.pkl"]
ML_THRESHOLDS = [128]#, 64, 128]  # Oil confidence thresholds for contour (int 0-255)

# XXX WARNING: chip_size_orig SHOULD ROUGHLY MATCH LENGTH SCALE OF MODEL TRAINING DATA
CHIP_SIZE_ORIG = 4096  # px square cut out of original Tiff (roughly 1/6th the long dimension of an image)
CHIP_SIZE_REDUCED = 512  # px square reduced resolution of chip_size
MAX_CHIP_QTY = None  # No limit to the number of chips
START_OVER = (
    True  # Do we delete all files before creating chips, or pick up where we left off
)
OVERHANG = False  # Should some chips hang over the edge of the original image (and therefore might have very few useful pixels)
RECORD_NONZEROS = (
    False  # Should the images with non-zero average pixel value be recorded in a CSV
)

# If the local_config module is found, import all those settings, overriding any here that overlap.
if importlib.util.find_spec("configs.local_config") is not None:
    from configs.local_config import *  # pylint: disable=unused-wildcard-import
