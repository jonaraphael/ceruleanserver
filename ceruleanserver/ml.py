from pathlib import Path
from datetime import datetime
from fastai2.learner import load_learner
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
    
    # Prepare some constants
    # XXX WARNING: chip_size SHOULD MATCH LENGTH SCALE OF MODEL TRAINING DATA
    chip_size = 4096 # px square cut out of original Tiff (roughly 1/6th the long dimension of an image)
    reduced_chip_size = 512 # px square reduced resolution of chip_size 
    max_chip_qty = 9999
    threshold = None # Between 0 (most) and 1 (least inclusive), which confidence should be considered oil
    start_over = True # Do we delete all files before creating chips, or pick up where we left off
    img_path = Path(snso.s3["grd_tiff_dest"])
    mask_dir = img_path.parent/'masks'
    merged_path = img_path.parent/'merged.tif'
    
    # Cut up the GTiff into many small TIFs
    create_chips(img_path, chip_size, reduced_chip_size, mask_dir, learner, threshold, max_chip_qty, start_over)
    # Merge the masks back into a single image
    merge_masks(mask_dir, merged_path)
    # Extract Polygons from the Merged file
        # Should we delete all the other intermediate files at this point?
    # Store Polygons in DB

def crop_box_gen(px_wide, px_high, chip_size, geo_transform=(0,1,0,0,0,1), overhang=False):
    """A generator that will sequentially return bounding corners of boxes to chop up a larger image into small ones
    
    Arguments:
        px_wide {int} -- Number of pixels wide to cover the larger image
        px_high {int} -- Number of pixels high to cover the larger image
        chip_size {int} -- Number of pixels on each side of the boxes

    Keyword Arguments:
        geo_transform {tuple} -- 6-value geotransform (default: {(0,1,0,0,0,1)}) https://gdal.org/user/raster_data_model.html#affine-geotransform
    
    Returns:
        [ulx, uly, lrx, lry] -- two corners of a box
    """
    num_wide = int(ceil(px_wide/chip_size))
    num_high = int(ceil(px_high/chip_size))
    if config.VERBOSE: print('Creating', num_wide*num_high,'Chips')

    for i in range(num_wide):
        for j in range(num_high):
            # These special cases are to avoid fractional images being processed, as they are not as likely to correctly interpret bilge slicks
            if not overhang and i==num_wide-1 and j==num_high-1:
                # very special case (corner)
                yield [
                    geo_transform[0]+(px_wide-chip_size)*geo_transform[1]+(px_high-chip_size)*geo_transform[2], # ULX: X_origin + box_number * num_pixels_per_box * size_of_X_pixels + 0
                    geo_transform[3]+(px_wide-chip_size)*geo_transform[4]+(px_high-chip_size)*geo_transform[5], # ULY: Y_origin + 0 + box_number * num_pixels_per_box * size_of_Y_pixels
                    geo_transform[0]+px_wide*geo_transform[1]+px_high*geo_transform[2], # LRX
                    geo_transform[3]+px_wide*geo_transform[4]+px_high*geo_transform[5]] # LRY
            elif not overhang and i==num_wide-1:
                # special case (edge)
                yield [
                    geo_transform[0]+(px_wide-chip_size)*geo_transform[1]+j*chip_size*geo_transform[2], # ULX: X_origin + box_number * num_pixels_per_box * size_of_X_pixels + 0
                    geo_transform[3]+(px_wide-chip_size)*geo_transform[4]+j*chip_size*geo_transform[5], # ULY: Y_origin + 0 + box_number * num_pixels_per_box * size_of_Y_pixels
                    geo_transform[0]+px_wide*geo_transform[1]+(j+1)*chip_size*geo_transform[2], # LRX
                    geo_transform[3]+px_wide*geo_transform[4]+(j+1)*chip_size*geo_transform[5]] # LRY
            elif not overhang and j==num_high-1:
                # special case (edge)
                yield [
                    geo_transform[0]+i*chip_size*geo_transform[1]+(px_high-chip_size)*geo_transform[2], # ULX: X_origin + box_number * num_pixels_per_box * size_of_X_pixels + 0
                    geo_transform[3]+i*chip_size*geo_transform[4]+(px_high-chip_size)*geo_transform[5], # ULY: Y_origin + 0 + box_number * num_pixels_per_box * size_of_Y_pixels
                    geo_transform[0]+(i+1)*chip_size*geo_transform[1]+px_high*geo_transform[2], # LRX
                    geo_transform[3]+(i+1)*chip_size*geo_transform[4]+px_high*geo_transform[5]] # LRY
            else:
                # normal case (bulk)
                yield [
                    geo_transform[0]+i*chip_size*geo_transform[1]+j*chip_size*geo_transform[2], # ULX: X_origin + box_number * num_pixels_per_box * size_of_X_pixels + 0
                    geo_transform[3]+i*chip_size*geo_transform[4]+j*chip_size*geo_transform[5], # ULY: Y_origin + 0 + box_number * num_pixels_per_box * size_of_Y_pixels
                    geo_transform[0]+(i+1)*chip_size*geo_transform[1]+(j+1)*chip_size*geo_transform[2], # LRX
                    geo_transform[3]+(i+1)*chip_size*geo_transform[4]+(j+1)*chip_size*geo_transform[5]] # LRY

def create_chips(img_path, chip_size, reduced_chip_size, mask_dir, mllearner, threshold=0.5, max_chip_qty=9999, start_over=True):
    """Turn a large image into small ones using GDAL
    
    Arguments:
        img_path {Path} -- Large GeoTiff to be chopped up
        chip_size {int} -- Number of pixels on a side (square) to divide the GTiff into
        reduced_chip_size {int} -- number of pixels on a side (square) lower-resolution version of chip_size
        chip_dir {Path} -- Folder where the chopped up PNGs will be stored
        max_chip_qty {int} -- Optional input to reduce computational load for testing
    """
    if mask_dir.exists() and start_over: # Delete any masks from a previous attempt
        shutil.rmtree(mask_dir)
    mask_dir.mkdir(exist_ok=True) # Create new directory for masks to be stored
    tiff = gdal.Open(str(img_path)) # Open the Large GeoTiff as a GDAL DataSet
    boxes = crop_box_gen(tiff.RasterXSize, tiff.RasterYSize, chip_size, tiff.GetGeoTransform()) # A list of pairs of corners (UL & LR) which will be cut from the large GeoTiff
    s = datetime.now() # Measure Inference time
    for i, b in enumerate(boxes): # 6.7s per 512px chip
        if i<max_chip_qty: # Stop short if artificially avoiding too many chips
            png_path = mask_dir/f'{img_path.stem}_{i}.png' # Create indexed name for new png to be stored
            if not png_path.exists() or start_over: # Avoid overwriting images already made
                gdal.Translate(str(png_path), tiff, format="PNG", width=reduced_chip_size, height=reduced_chip_size, projWin=b, outputType=gdal.GDT_Byte) # check out more args here: https://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
                infer(png_path, mllearner, threshold) # Run Inference on the current chip
                if config.VERBOSE: print(png_path.stem, (datetime.now()-s)/(i+1))
    del tiff

def infer(png_path, learner, threshold = None):
    """Run inference on a chip

    Arguments:
        png_path {Path} -- Location of a single chip
        learner {fastai2 model} -- Loaded fastai2 pkl file

    Keyword Arguments:
        threshold {float} -- Round predicted confidence above thresh to 1 and below to 0 (default: {None})
    """    
    _, _, pred_class  = learner.predict(png_path) # Currently returns classes [not bilge, bilge, vessel]
    mask = where(pred_class[1]>threshold, 1, 0) if threshold else pred_class[1]
    PILimage.fromarray(uint8(mask*255)).save(png_path)
    gdal.Warp(destNameOrDestDS=str(png_path.with_suffix('.tif')), srcDSOrSrcDSTab=str(png_path), srcNodata=0, dstNodata=0, format='GTiff')

    # gdal.Translate(str(png_path.with_suffix('.tif')), str(png_path), format="GTiff")

    # tif = gdal.Open(str(png_path), gdal.GA_Update)
    # outBand = tif.GetRasterBand(1)
    # gdal_array.BandWriteArray(outBand, pred_class[1].numpy())
    # outBand.WriteArray(pred_class[1].numpy())
    # outBand.FlushCache()
    # del tif

def merge_masks(mask_dir, merged_path):
    """Merge multiple chips into one large tif
    
    Arguments:
        mask_dir {Path} -- Folder where multiple PNGs are stored
        merged_path {Path} -- File where the merged TIF will be generated
    """
    if config.VERBOSE: print('Merging Masks')
    if (merged_path).exists():
        (merged_path).unlink() # Delete previously file from previous merge attempt
    cmd = f'gdal_merge.py -o {merged_path} {mask_dir}/*.tif'
    os.system(cmd)
    shutil.rmtree(mask_dir)

def load_learner_from_s3():
    """Import the latest trained model from S3

    Returns:
        Learner -- fastai2 trained model loaded into memory
    """    
    if config.VERBOSE: print('Loading Learner')
    pkl_name = 'ml.pkl'
    pkl_dir = Path('./temp/')
    if config.UPDATE_ML and (pkl_dir/pkl_name).exists():
        (pkl_dir/pkl_name).unlink() # pylint: disable=no-member
    if not (pkl_dir/pkl_name).exists(): # pylint: disable=no-member
        download_str = f'aws s3 cp {config.ML_PKL} {pkl_dir/pkl_name}'
        # print(download_str)
        os.system(download_str)
    l = load_learner(pkl_dir/pkl_name)
    return l

def get_lbls(): return # Required by fastai as it was used during creation of the model