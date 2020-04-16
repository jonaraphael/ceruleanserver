#%%
import config
from fastai.vision import Path
from PIL import Image as PILimage
from math import ceil
from os import makedirs
from datetime import datetime
from shutil import copyfile, copytree



def crop_box_gen(num_wide, num_high, chip_size):
  for i in range(num_wide):
    for j in range(num_high):
      yield (j*chip_size, i*chip_size, (j+1)*chip_size, (i+1)*chip_size)

def create_chips(img_path, chip_size, out_path=None):
  chip_path = out_path or img_path.parent/f'{chip_size}_chips'
  makedirs(chip_path, exist_ok=True)
  with PILimage.open(img_path) as img:
    num_wide = ceil(img.width/chip_size)
    num_high = ceil(img.height/chip_size)
    boxes = crop_box_gen(num_wide, num_high, chip_size)
    for i, b in enumerate(boxes):
      fname = f'{img_path.stem}_{i:09}.png'
      img.crop(b).save(chip_path/fname, 'PNG')
      # print(f'Saved {chip_path/fname}')

# %%
