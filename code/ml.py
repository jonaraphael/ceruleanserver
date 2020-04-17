from fastai.vision import Path
from math import ceil
from os import makedirs
from datetime import datetime
from osgeo import gdal

def ml_ingest(snso):
  chip_size = 1000 # px square
  s = datetime.now()
  create_chips(snso.s3["grd_tiff_dest"], chip_size)
  print(datetime.now()-s)

def crop_box_gen(num_wide, num_high, chip_size):
  for i in range(num_wide):
    for j in range(num_high):
      yield [j*chip_size, i*chip_size, (j+1)*chip_size, (i+1)*chip_size]

def create_chips(img_path, chip_size, out_path=None):
  img_path = Path(img_path)
  chip_path = Path(out_path) if out_path else img_path.parent/f'{chip_size}_chips'
  makedirs(chip_path, exist_ok=True)
  tiff = gdal.Open(str(img_path))
  num_wide = ceil(tiff.RasterXSize/chip_size)
  num_high = ceil(tiff.RasterYSize/chip_size)
  boxes = crop_box_gen(num_wide, num_high, chip_size)
  for i, b in enumerate(boxes):
    fname = chip_path/f'{img_path.stem}_{i}.png'
    gdal.Translate(str(fname), tiff, format="PNG", projWin=b) # check out more args here: https://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
  del tiff