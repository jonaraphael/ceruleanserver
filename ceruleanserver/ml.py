from pathlib import Path
from datetime import datetime
from fastai2.learner import load_learner
from osgeo import gdal
from numpy import zeros, floor, ceil, uint8, where, array, dstack
from PIL import Image as PILimage
import shutil
import configs.serverconfig as config
import os
import csv


def machine(learner, snso):
    """Run machine learning on an SNS object
    
    Arguments:
        snso {SNSO} -- a new image that we have determined to be of interest (e.g. over the ocean and using VV polarization)
    """
    # Prepare some constants
    # XXX WARNING: chip_size_orig SHOULD ROUGHLY MATCH LENGTH SCALE OF MODEL TRAINING DATA
    chip_size_orig = 4096  # px square cut out of original Tiff (roughly 1/6th the long dimension of an image)
    chip_size_reduced = 512  # px square reduced resolution of chip_size
    max_chip_qty = None  # No limit to the number of chips
    threshold = None  # Between 0 (most) and 1 (least inclusive), which confidence should be considered oil
    start_over = True  # Do we delete all files before creating chips, or pick up where we left off
    overhang = False  # Should some chips hang over the edge of the original image (and therefore might have very few useful pixels)
    record_nonzeros = False  # Should the images with non-zero average pixel value be recorded in a CSV
    img_path = snso.s3["grd_tiff_dest"]  # Where the new image is
    inf_dir = img_path.parent / "inf"  # Where the inference chips should be stored
    merged_path = (
        img_path.parent / "merged.tif"
    )  # Where the merged inference mask should be stored

    # Download Large GeoTiff
    snso.download_grd_tiff()
    # Cut up the GTiff into many small TIFs
    img_to_chips(
        img_path,
        inf_dir,
        chip_size_orig,
        chip_size_reduced,
        img_path.stem,
        overhang,
        max_chip_qty,
        start_over,
        record_nonzeros,
        learner,
        threshold,
    )
    # Merge the masks back into a single image
    merge_chips(inf_dir, merged_path)
    # Extract Polygons from the Merged file
    pass  ### Should we delete all the other intermediate files at this point?
    # Store Polygons in DB
    pass  ###


def crop_box_gen(
    px_wide, px_high, chip_size, geo_transform=(0, 1, 0, 0, 0, 1), overhang=False
):
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
    num_wide = int(ceil(px_wide / chip_size))
    num_high = int(ceil(px_high / chip_size))
    if config.VERBOSE:
        print("Creating", num_wide * num_high, "Chips")

    for i in range(num_wide):
        for j in range(num_high):
            x_vals = (
                [i * chip_size, (i + 1) * chip_size]
                if (i != num_wide - 1) or overhang
                else [px_wide - chip_size, px_wide]
            )
            y_vals = (
                [j * chip_size, (j + 1) * chip_size]
                if (j != num_high - 1) or overhang
                else [px_high - chip_size, px_high]
            )
            yield [
                geo_transform[0]
                + x_vals[0] * geo_transform[1]
                + y_vals[0]
                * geo_transform[
                    2
                ],  # ULX: X_origin + box_number * num_pixels_per_box * size_of_X_pixels + 0
                geo_transform[3]
                + x_vals[0] * geo_transform[4]
                + y_vals[0]
                * geo_transform[
                    5
                ],  # ULY: Y_origin + 0 + box_number * num_pixels_per_box * size_of_Y_pixels
                geo_transform[0]
                + x_vals[1] * geo_transform[1]
                + y_vals[1] * geo_transform[2],  # LRX
                geo_transform[3]
                + x_vals[1] * geo_transform[4]
                + y_vals[1] * geo_transform[5],
            ]  # LRY


def img_to_chips(
    img_path,
    out_dir,
    chip_size_orig,
    chip_size_reduced,
    out_stem=None,
    overhang=False,
    max_chip_qty=None,
    start_over=True,
    record_nonzeros=False,
    mllearner=None,
    threshold=None,
):
    """Turn a large image into small ones using GDAL

    Arguments:
        img_path {Path} -- Location of a large image (TIFF or PNG)
        out_dir {Path} -- Directory where chips should be delivered
        chip_size_orig {int} -- Number of pixels on a side (square) to divide the GTiff into
        chip_size_reduced {int} -- number of pixels on a side (square) lower-resolution version of chip_size

    Keyword Arguments:
        out_stem {str} -- Basis for chip naming convention (default: {img_path.stem})
        overhang {bool} -- Should remainder pixels be orphaned into mostly-empty chips? (default: {False})
        max_chip_qty {int} -- Number of chips to produce, or None for no limit (default: {None})
        start_over {bool} -- Do you want to preserve chips from previous runs of this function? (default: {True})
        record_nonzeros {bool} -- Do you want to record which chips contain non-zero pixels? (default: {False})
        mllearner {fastai2_learner} -- The loaded learner if doing inference, else None (default: {None})
        threshold {float} -- The level of confidence to split inference into binary. If None, then outputs grayscale (default: {None})
    """
    if not img_path.exists():  # pylint: disable=no-member
        print(
            "ERROR: No image found:", img_path
        )  # We need an image if you want to chip it
        return
    if config.VERBOSE:
        print("Chipping", img_path.name, "into", out_dir)
    out_dir.mkdir(
        parents=True, exist_ok=True
    )  # Create new directory for chips to be stored
    out_stem = (
        out_stem or img_path.stem
    )  # Default the output names to be the same as the input image

    img = gdal.Open(str(img_path))  # Open the large image as a GDAL DataSet
    boxes = crop_box_gen(
        img.RasterXSize,
        img.RasterYSize,
        chip_size_orig,
        img.GetGeoTransform(),
        overhang,
    )  # A list of pairs of corners (UL & LR) which will be cut from the large GeoTiff
    s = datetime.now()  # Measure processing time
    for i, b in enumerate(boxes):  # for each box
        if (max_chip_qty is None) or (
            i < max_chip_qty
        ):  # Stop short if artificially avoiding too many chips
            out_path = (
                out_dir / f"{out_stem}_{i}.png"
            )  # Create indexed name for new png to be stored
            if (
                start_over or not out_path.exists()
            ):  # Avoid overwriting images already made
                gdal.Translate(
                    str(out_path),
                    img,
                    format="PNG",
                    width=chip_size_reduced,
                    height=chip_size_reduced,
                    projWin=b,
                    outputType=gdal.GDT_Byte,
                )  # check out more args here: https://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
                if mllearner:  # If model provided
                    infer(
                        out_path, mllearner, threshold
                    )  # Run Inference on the current chip
                if record_nonzeros:  # If recording nonzero chips
                    chp = gdal.Open(str(out_path))  # Open each chip
                    stats = chp.GetRasterBand(1).GetStatistics(
                        0, 1
                    )  # Calculate the stats on that chip
                    if stats[1] > 0:  # If the average pixel value is > 0
                        with open(
                            out_dir / "nonzeros.csv", "a"
                        ) as f:  # Open a local CSV
                            csv.writer(f).writerow(
                                [out_path.name]
                            )  # Record the chip name
                    del chp  # Remove the chip from memory
        if config.VERBOSE:
            print(
                out_path.stem, (datetime.now() - s) / (i + 1)
            )  # Inference around 6.7s per 512px chip
    del img


def infer(png_path, learner, threshold=None):
    """Run inference on a chip

    Arguments:
        png_path {Path} -- Location of a single chip
        learner {fastai2 model} -- Loaded fastai2 pkl file

    Keyword Arguments:
        threshold {float} -- Round predicted confidence above thresh to 1 and below to 0 (default: {None})
    """
    _, _, pred_class = learner.predict(
        png_path
    )  # Currently returns classes [not bilge, bilge, vessel]
    mask = where(pred_class[1] > threshold, 1, 0) if threshold else pred_class[1]
    PILimage.fromarray(uint8(mask * 255)).save(png_path)
    gdal.Warp(
        destNameOrDestDS=str(png_path.with_suffix(".tif")),
        srcDSOrSrcDSTab=str(png_path),
        srcNodata=0,
        dstNodata=0,
        format="GTiff",
    )

    # gdal.Translate(str(png_path.with_suffix('.tif')), str(png_path), format="GTiff")

    # tif = gdal.Open(str(png_path), gdal.GA_Update)
    # outBand = tif.GetRasterBand(1)
    # gdal_array.BandWriteArray(outBand, pred_class[1].numpy())
    # outBand.WriteArray(pred_class[1].numpy())
    # outBand.FlushCache()
    # del tif


def merge_chips(chp_dir, merged_path, chip_ext="tif"):
    """Merge multiple chips into one large tif
    
    Arguments:
        chp_dir {Path} -- Folder where multiple PNGs are stored
        merged_path {Path} -- File where the merged TIF will be generated

    Keyword Arguments:
        chip_ext {str} -- The file extension of the chips to be merged (default: {"tif"})
    """
    if config.VERBOSE:
        print("Merging Masks")
    if (merged_path).exists():
        (
            merged_path
        ).unlink()  # Delete file from previous merge attempt (gdal_merge will NOT overwrite!)
    # gdal.Warp(destNameOrDestDS=str(merged_path), srcDSOrSrcDSTab=[str(p) for p in chp_dir.glob('*.png')], format='GTiff') # This doesn't work because PNG clamps to [0-255], and no_data makes some pixels invisible
    cmd = f"gdal_merge.py -o {merged_path} {chp_dir}/*.{chip_ext}"
    os.system(cmd)
    shutil.rmtree(chp_dir)


def scale(b, arr):
    """Scale OCN bands by their mins/maxes

    Arguments:
        b {dict} -- A dictionary that contains 4 floats: min, max, no_data_in, no_data_out
        arr {np.array} -- A 2D matrix of numbers representing the corresponding raw band from an OCN

    Returns:
        np.array -- A 2D matrix representing a scaled version of the input array
    """
    no_data = where(arr == b["no_data_in"])
    res = 255 * ((arr - b["min"]) / (b["max"] - b["min"]))
    res[no_data] = b["no_data_out"]
    res = array([[uint8(a) for a in b] for b in res])
    return res


def nc_to_png(nc_path, bands, target_size, out_path=None):
    """Convert a NetCDF file into a scaled PNG with channels from the bands dictionary

    Arguments:
        nc_path {Path} -- Where the .nc file is stored
        bands {dict} -- Dictionary of bands to be extracted
        target_size {(int,int)} -- 2-tuple number of pixels wide and high for the output

    Keyword Arguments:
        out_path {Path} -- Where to store the output png (default: {nc_path.with_name('nc_rast.png')})

    Returns:
        out_path
    """
    out_path = out_path or nc_path.with_name("nc_rast.png")
    out = []  # Initialize
    for b in bands:  # For the bands we care about (defined in a JSON)
        owi = gdal.Open(
            f"NETCDF:{nc_path}:{b}"
        )  # Open the band (GDAL fails if this is moved inside the next line)
        out.append(
            scale(bands[b], owi.GetRasterBand(1).ReadAsArray())
        )  # Scale the band between 0 and 255
        del owi  # Remove the gdal object from memory
    (
        PILimage.fromarray(dstack(out))  # Turn the raster arrays into an image
        .resize(target_size)  # Resize to match Training Data dimensions
        .transpose(
            PILimage.FLIP_TOP_BOTTOM
        )  # Flip the Y axis (not clear why this is necessary, or if it needs exceptions)
        .save(out_path)
    )  # Save the file
    return out_path  # Return the path of the new file


def load_learner_from_s3():
    """Import the latest trained model from S3

    Returns:
        Learner -- fastai2 trained model loaded into memory
    """
    if config.VERBOSE:
        print("Loading Learner")
    pkl_name = "ml.pkl"
    pkl_dir = Path("./temp/")
    if config.UPDATE_ML and (pkl_dir / pkl_name).exists():
        (pkl_dir / pkl_name).unlink()  # pylint: disable=no-member
    if not (pkl_dir / pkl_name).exists():  # pylint: disable=no-member
        download_str = f"aws s3 cp {config.ML_PKL} {pkl_dir/pkl_name}"
        # print(download_str)
        os.system(download_str)
    l = load_learner(pkl_dir / pkl_name)
    return l


def get_lbls():
    return  # Required by fastai as it was used during creation of the model

