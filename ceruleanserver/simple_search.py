#%%
from pathlib import Path
from classes import SHO
from ml.inference import INFERO
from configs import path_config, server_config
from unittest.mock import Mock

pids = [
    # "S1A_IW_GRDH_1SDV_20190101T193354_20190101T193421_025288_02CC18_DB4C",
    # "S1A_IW_GRDH_1SDV_20200403T023909_20200403T023934_031957_03B0B8_76CD",
    # "S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112",
    # "S1B_IW_GRDH_1SDV_20200217T062710_20200217T062735_020305_026740_E3B6",
    # "S1B_IW_GRDH_1SDV_20200415T160706_20200415T160731_021157_028259_9E75",
    # Found by machine:
    # "S1A_IW_GRDH_1SDV_20200607T042337_20200607T042402_032906_03CFC9_3C1C", #
    # "S1A_IW_GRDH_1SDV_20200607T101743_20200607T101812_032909_03CFE2_9B48", #
    # "S1A_IW_GRDH_1SDV_20200607T152734_20200607T152759_032913_03CFFD_5931", #
]
mocksnso = Mock()

for pid in pids:
    grd_path = Path(path_config.LOCAL_DIR) / "temp" / pid / "vv_grd.tiff"
    if (
        not grd_path.exists()
    ) and server_config.DOWNLOAD_GRDS:  # pylint: disable=no-member
        SHO(pid).download_grd_tiff_from_s3(grd_path)
    mocksnso.configure_mock(grd_path=grd_path, prod_id=pid)
    out_path = INFERO(mocksnso).run_inference()
    print(out_path)

# %%
