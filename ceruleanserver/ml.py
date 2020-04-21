from pathlib import Path
from math import ceil
from os import system
from datetime import datetime
from osgeo import gdal

def machine(snso):
    """Run machine learning on an SNS object
    
    Arguments:
        snso {SNSO} -- a new image that we have determined to be of interest (e.g. over the ocean and using VV polarization)
    """
    # Download Large GeoTiff
    snso.download_grd_tiff()

    # Cut up the GTiff into many small PNGs
    chip_size = 1000 # px square cut out of original Tiff
    reduced_chip_size = 100 # px square reduced resolution of chip_size
    img_path = Path(snso.s3["grd_tiff_dest"])
    chip_path = img_path.parent/f'{chip_size}_{reduced_chip_size}_chips'
    create_chips(img_path, chip_size, reduced_chip_size, chip_path)

    # Run inference on chips
        # Can we save space by deleting the original TIFF?
        # Can we save space by replacing each PNG with the ML pred output?
        # Could we make two separate passes through at different resolutions?

    # Merge the chips back into a single image
    merged_path = chip_path/'merged.tif'
    merge_chips(chip_path, merged_path)

    # Extract Polygons from the Merged file
        # Should we delete all the other intermediate files at this point?

    # Store Polygons in DB

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

def create_chips(img_path, chip_size, reduced_chip_size, chip_path):
    """Turn a large image into small ones using GDAL
    
    Arguments:
        img_path {Path} -- Large GeoTiff to be chopped up
        chip_size {int} -- Number of pixels on a side (square) to divide the GTiff into
        reduced_chip_size {int} -- number of pixels on a side (square) lower-resolution version of chip_size
        chip_path {Path} -- Folder where the chopped up PNGs will be stored
    """
    pct = reduced_chip_size/chip_size*100
    chip_path.mkdir(exist_ok=True)
    tiff = gdal.Open(str(img_path))
    num_wide = ceil(tiff.RasterXSize/chip_size)
    num_high = ceil(tiff.RasterYSize/chip_size)
    boxes = crop_box_gen(num_wide, num_high, chip_size)
    for i, b in enumerate(boxes):
        fname = chip_path/f'{img_path.stem}_{i}.png'
        gdal.Translate(str(fname), tiff, format="PNG", widthPct=pct, heightPct=pct, projWin=b) # check out more args here: https://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
    del tiff

def merge_chips(chip_path, merged_path):
    """Merge multiple chips into one large tif
    
    Arguments:
        chip_path {Path} -- Folder where multiple PNGs are stored
        merged_path {Path} -- File where the merged TIF will be generated
    """        
    chip_path = Path(chip_path)
    png_list = [png for png in chip_path.glob("*.png")]
    tif_list = [str(p.with_suffix('.tif')) for p in png_list] 
    for png in png_list:
        tif = png.with_suffix('.tif')
        gdal.Translate(str(tif), str(png), format="GTiff")
        gdal.Warp(destNameOrDestDS=str(tif), srcDSOrSrcDSTab=str(tif), srcNodata=0)
    if (merged_path).exists():
        (merged_path).unlink()
    cmd = f'gdal_merge.py -o {str(merged_path)} {" ".join(tif_list)}'
    # print(cmd)
    system(cmd) # XXX WARNING Is this limited in the length of cmd?! 
