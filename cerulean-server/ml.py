from pathlib import PurePath as Path
from math import ceil
from os import makedirs
from datetime import datetime
from osgeo import gdal

def machine(snso):
  """Run machine learning on an SNS object
  
  Arguments:
      snso {SNSO} -- a new image that we have determined to be of interest (e.g. over the ocean and using VV polarization)
  """  
  snso.download_grd_tiff()
  chip_size = 1000 # px square
  pct = 10 # percent or original file size
  # Output_chip_size = chip_size*pct/100
  create_chips(snso.s3["grd_tiff_dest"], chip_size, pct)
  # Run inference on chips
  # Can we save space by deleting the original TIFF?
  # Can we save space by replacing each PNG with the ML pred output?

def crop_box_gen(num_wide, num_high, chip_size):
  """A generator that will sequentially return bounding corners of boxes to chop up a larger image into small ones
  
  Arguments:
      num_wide {int} -- Number of chips wide to cover the larger image
      num_high {int} -- Number of chips high to cover the larger image
      chip_size {int} -- Number of pixels on each side of the boxes
  
  Returns:
    [ulx, uly, lrx, lry] -- two corners of a box
  """    
  for i in range(num_wide):
    for j in range(num_high):
      yield [i*chip_size, j*chip_size, (i+1)*chip_size, (j+1)*chip_size]

def create_chips(img_path, chip_size, pct, out_path=None):
  """Turn a large image into small ones using GDAL
  
  Arguments:
      img_path {str} -- location of GeoTiff
      chip_size {int} -- number of pixels on a side (square) to divide the GTiff into
      pct {float} -- 100 means same resolution, 10% means each output chip side has 10% of chip_size pixels per side
  
  Keyword Arguments:
      out_path {str} -- optional path where to store the chips (default: {None})
  """  
  img_path = Path(img_path)
  chip_path = Path(out_path) if out_path else img_path.parent/f'{chip_size}_chips'
  makedirs(chip_path, exist_ok=True)
  tiff = gdal.Open(str(img_path))
  num_wide = ceil(tiff.RasterXSize/chip_size)
  num_high = ceil(tiff.RasterYSize/chip_size)
  boxes = crop_box_gen(num_wide, num_high, chip_size)
  for i, b in enumerate(boxes):
    fname = chip_path/f'{img_path.stem}_{i}.png'
    gdal.Translate(str(fname), tiff, format="PNG", widthPct=pct, heightPct=pct, projWin=b) # check out more args here: https://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
  del tiff