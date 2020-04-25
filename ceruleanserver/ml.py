from pathlib import Path
from datetime import datetime
from fastai.vision import load_learner, get_image_files, open_image, Image
from osgeo import gdal
from numpy import zeros, floor, ceil, uint8, where
from PIL import Image as PILimage
import shutil
import config
import os

def machine(learner, snso):
    """Run machine learning on an SNS object
    
    Arguments:
        snso {SNSO} -- a new image that we have determined to be of interest (e.g. over the ocean and using VV polarization)
    """
    # Download Large GeoTiff
    snso.download_grd_tiff()

    # Cut up the GTiff into many small PNGs
    chip_size = 512*4 # px square cut out of original Tiff
    reduced_chip_size = 512 # px square reduced resolution of chip_size
    img_path = Path(snso.s3["grd_tiff_dest"])
    chip_dir = img_path.parent/'chips'
    max_chip_qty = 99999
    create_chips(img_path, chip_size, reduced_chip_size, chip_dir, max_chip_qty)

    # Run inference on chips
    mask_dir = img_path.parent/'masks'
    threshold = .2 # Between 0 (most) and 1 (least inclusive), which confidence should be considered oil
    infer(learner, chip_dir, mask_dir, threshold, max_chip_qty)
        # Can we save space by deleting the original TIFF?
        # Can we save space by replacing each PNG with the ML pred output?
        # Could we make two separate passes through at different resolutions?

    # Merge the masks back into a single image
    merged_path = mask_dir/'merged.tif'
    merge_masks(mask_dir, merged_path)

    # Extract Polygons from the Merged file
        # Should we delete all the other intermediate files at this point?

    # Store Polygons in DB

def crop_box_gen(num_wide, num_high, chip_size, geo_transform=(0,1,0,0,0,1)):
    """A generator that will sequentially return bounding corners of boxes to chop up a larger image into small ones
    
    Arguments:
        num_wide {int} -- Number of chips wide to cover the larger image
        num_high {int} -- Number of chips high to cover the larger image
        chip_size {int} -- Number of pixels on each side of the boxes

    Keyword Arguments:
        geo_transform {tuple} -- 6-value geotransform (default: {(0,1,0,0,0,1)}) https://gdal.org/user/raster_data_model.html#affine-geotransform
    
    Returns:
        [ulx, uly, lrx, lry] -- two corners of a box
    """
    for i in range(num_wide):
        for j in range(num_high):
            yield [
                geo_transform[0]+i*chip_size*geo_transform[1], # ULX: X_origin + box_number * num_pixels_per_box * size_of_X_pixels
                geo_transform[3]+j*chip_size*geo_transform[5], # ULY: Y_origin + box_number * num_pixels_per_box * size_of_Y_pixels
                geo_transform[0]+(i+1)*chip_size*geo_transform[1], # LRX
                geo_transform[3]+(j+1)*chip_size*geo_transform[5]] # LRY

def create_chips(img_path, chip_size, reduced_chip_size, chip_dir, max_chip_qty=9999):
    """Turn a large image into small ones using GDAL
    
    Arguments:
        img_path {Path} -- Large GeoTiff to be chopped up
        chip_size {int} -- Number of pixels on a side (square) to divide the GTiff into
        reduced_chip_size {int} -- number of pixels on a side (square) lower-resolution version of chip_size
        chip_dir {Path} -- Folder where the chopped up PNGs will be stored
        max_chip_qty {int} -- Optional input to reduce computational load for testing
    """
    if config.VERBOSE: print('Creating Chips')
    pct = reduced_chip_size/chip_size*100
    chip_dir.mkdir(exist_ok=True)

    warped_path = img_path.with_name('vv_grd_warped.tiff')
    if not warped_path.exists():
        if config.VERBOSE: print('Warping Tiff')
        gdal.Warp(destNameOrDestDS=str(warped_path), srcDSOrSrcDSTab=str(img_path), srcNodata=0, dstNodata=0)
    tiff = gdal.Open(str(warped_path))
    num_wide = ceil(tiff.RasterXSize/chip_size).astype('int')
    num_high = ceil(tiff.RasterYSize/chip_size).astype('int')
    geo_transform = tiff.GetGeoTransform()
    boxes = crop_box_gen(num_wide, num_high, chip_size, geo_transform)
    for i, b in enumerate(boxes):
        if i<max_chip_qty:
            if config.VERBOSE and i%10==0: print(i)
            fname = chip_dir/f'{img_path.stem}_{i}.png'
            gdal.Translate(str(fname), tiff, format="PNG", widthPct=pct, heightPct=pct, projWin=b) # check out more args here: https://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
    del tiff

def merge_masks(mask_dir, merged_path):
    """Merge multiple chips into one large tif
    
    Arguments:
        mask_dir {Path} -- Folder where multiple PNGs are stored
        merged_path {Path} -- File where the merged TIF will be generated
    """
    if config.VERBOSE: print('Merging Masks')
    png_list = [png for png in mask_dir.glob("*.png")]
    tif_list = [str(p.with_suffix('.tif')) for p in png_list] 
    for png in png_list:
        tif = png.with_suffix('.tif')
        gdal.Translate(str(tif), str(png), format="GTiff")
    if (merged_path).exists():
        (merged_path).unlink()
    cmd = f'gdal_merge.py -o {str(merged_path)} {" ".join(tif_list)}'
    # print(cmd)
    os.system(cmd) # XXX WARNING Does this limit the length of cmd by # chars?! 

def infer(learner, chip_dir, mask_dir, threshold, max_chip_qty=9999):
    if config.VERBOSE: print('Running Inference')
    fnames = get_image_files(chip_dir)
    mask_dir.mkdir(exist_ok=True)
    s = datetime.now()
    for i, f in enumerate(fnames): # 6.9 seconds per chip?!
        if i<max_chip_qty:
            y = learner.predict(open_image(f)) 
            binary = uint8(where(y[2][1]>threshold,255,0))
            PILimage.fromarray(binary).save(mask_dir/f.name, 'PNG')
            aux_file = f.with_suffix('.png.aux.xml')
            shutil.copy(str(aux_file), str(mask_dir/aux_file.name))
            if config.VERBOSE: print(i, f.stem, (datetime.now()-s)/(i+1))

def load_learner_from_s3():
    if config.VERBOSE: print('Loading Learner')
    pkl_name = 'ml.pkl'
    pkl_dir = Path('./temp/')
    if config.UPDATE_ML:
        (pkl_dir/pkl_name).unlink() # pylint: disable=no-member
    if not (pkl_dir/pkl_name).exists(): # pylint: disable=no-member
        download_str = f'aws s3 cp {config.ML_PKL} {pkl_dir/pkl_name}'
        # print(download_str)
        os.system(download_str)
    l = load_learner(pkl_dir, file=pkl_name)
    return l