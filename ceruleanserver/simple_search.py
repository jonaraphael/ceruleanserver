#%%
from pathlib import Path
from classes import SHO
from ml.inference import INFERO, get_lbls
from configs import path_config, server_config

pids = [
    "S1A_IW_GRDH_1SDV_20190101T193354_20190101T193421_025288_02CC18_DB4C",
    # "S1A_IW_GRDH_1SDV_20200403T023909_20200403T023934_031957_03B0B8_76CD",
    # "S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112",
    # "S1B_IW_GRDH_1SDV_20200217T062710_20200217T062735_020305_026740_E3B6",
    # "S1B_IW_GRDH_1SDV_20200415T160706_20200415T160731_021157_028259_9E75",
]

for pid in pids:
    grd_path = Path(path_config.LOCAL_DIR) / "temp" / pid / "vv_grd.tiff"
    if not grd_path.exists() and server_config.DOWNLOAD_GRDS:  # pylint: disable=no-member
        SHO(pid).download_grd_tiff_from_s3(grd_path)
    out_path = INFERO(grd_path, pid).run_inference()
    print(out_path)

# %%
