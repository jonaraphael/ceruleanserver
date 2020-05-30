#%%
from multi_inf import multi_machine
from inference import get_lbls

pids = [
    "S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112",
    "S1A_IW_GRDH_1SDV_20190101T193354_20190101T193421_025288_02CC18_DB4C",
    "S1A_IW_GRDH_1SDV_20200403T023909_20200403T023934_031957_03B0B8_76CD",
    "S1B_IW_GRDH_1SDV_20200217T062710_20200217T062735_020305_026740_E3B6",
    "S1B_IW_GRDH_1SDV_20200415T160706_20200415T160731_021157_028259_9E75",
]

pkls = ["2_18_128_0.676.pkl", "2_18_256_0.691.pkl", "2_18_512_0.705.pkl"]
thresh = [32, 64, 128]  # Confidence threshold for contour(between 0 and 255)

for pid in pids:
    print(pid)
    out = multi_machine(pid, pkls, thresh)
    print(out.name)

# %%
