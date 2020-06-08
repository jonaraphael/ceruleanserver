from osgeo import gdal
from pathlib import Path
from datetime import datetime
from subprocess import run, PIPE
from numpy import ceil, uint8, where, array, dstack
from PIL import Image as PILimage
import sys
import csv
import shutil

sys.path.append(str(Path(__file__).parent.parent))
from configs import server_config
from utils.common import clear

gdal.UseExceptions()
# https://gdal.org/api/python_gotchas.html
# https://trac.osgeo.org/gdal/wiki/LayerAlgebra


def crop_box_gen(
    px_wide, px_high, chip_size, overhang, geo_transform=(0, 1, 0, 0, 0, 1),
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
    if server_config.VERBOSE:
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


def merge_chips(chp_dir, merged_path, chip_ext="tiff"):
    """Merge multiple chips into one large tiff
    
    Arguments:
        chp_dir {Path} -- Folder where multiple PNGs are stored
        merged_path {Path} -- File where the merged TIF will be generated

    Keyword Arguments:
        chip_ext {str} -- The file extension of the chips to be merged (default: {"tiff"})
    """
    if server_config.VERBOSE:
        print("Merging Masks")
    clear(
        merged_path
    )  # Delete file from previous merge attempt (gdal_merge will NOT overwrite!)
    cmd = f"gdal_merge.py -o {merged_path} {chp_dir}/*.{chip_ext}"  # XXXJona protect cmds against spaces in file paths
    run(cmd, stdout=PIPE, shell=True)
    shutil.rmtree(chp_dir)


def scale(band, arr):
    """Scale OCN bands by their mins/maxes

    Arguments:
        band {dict} -- A dictionary that contains 4 floats: min, max, no_data_in, no_data_out
        arr {np.array} -- A 2D matrix of numbers representing the corresponding raw band from an OCN

    Returns:
        np.array -- A 2D matrix representing a scaled version of the input array
    """
    no_data = where(arr == band["no_data_in"])
    res = 255 * ((arr - band["min"]) / (band["max"] - band["min"]))
    res[no_data] = band["no_data_out"]
    res = array([[uint8(x) for x in y] for y in res])
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


def inference_to_poly(raster_path, threshold, out_path=None):
    out_path = out_path or raster_path.with_name(str(threshold)+"_"+raster_path.stem+".geojson")
    intermediary_path = raster_path.with_name("intermediary.tiff")
    clear(out_path)

    cmd = f"gdal_calc.py -A '{raster_path}' --outfile='{intermediary_path}' --calc='1*(A>={threshold})' --NoDataValue=0"
    run(cmd, stdout=PIPE, shell=True)

    cmd = f"gdal_polygonize.py '{intermediary_path}' -f GeoJSON '{out_path}'"
    run(cmd, stdout=None if server_config.VERBOSE else PIPE, shell=True)

    clear(intermediary_path)
    return out_path


def inference_to_geotiff(inference, png_path, target_size=None, out_path=None):
    out_path = out_path or png_path.with_suffix(".tiff")

    pil = PILimage.fromarray(uint8(inference * 255))
    if target_size:
        pil = pil.resize((target_size, target_size))
    pil.save(png_path)
    gdal.Warp(
        destNameOrDestDS=str(out_path),
        srcDSOrSrcDSTab=str(png_path),
        srcNodata=0,
        dstNodata=0,
        format="GTiff",
    )
    return out_path


def img_chip_generator(
    img_path,
    chip_size_orig,
    chip_size_reduced,
    overhang=False,
    start_over=True,
    max_chip_qty=None,
    record_nonzeros=False,
    out_dir=None,
    out_stem=None,
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
    out_dir = out_dir or img_path.parent / "chps"
    if server_config.VERBOSE:
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
        overhang,
        img.GetGeoTransform(),
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
            if record_nonzeros:  # If recording nonzero chips
                chp = gdal.Open(str(out_path))  # Open each chip
                stats = chp.GetRasterBand(1).GetStatistics(
                    0, 1
                )  # Calculate the stats on that chip
                if stats[1] > 0:  # If the average pixel value is > 0
                    with open(out_dir / "nonzeros.csv", "a") as f:  # Open a local CSV
                        csv.writer(f).writerow([out_path.name])  # Record the chip name
                del chp  # Remove the chip from memory
            yield out_path
    del img
