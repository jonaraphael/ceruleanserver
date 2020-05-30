#%%
from osgeo import gdal, ogr, osr
from pathlib import Path
import sys
from inference import machine, load_learner_from_s3, get_lbls
from subprocess import run, PIPE

sys.path.append(str(Path(__file__).parent.parent))
from classes import SHO
from configs import path_config, server_config, ml_config

gdal.UseExceptions()
# https://gdal.org/api/python_gotchas.html
# https://trac.osgeo.org/gdal/wiki/LayerAlgebra

#%%
# Define Useful Functions


def clear(p):
    """Delete file if it exists

    Arguments:
        p {Path} -- file to be deleted
    """
    # This function can be replced by Path.unlink.(missing_ok=True) when we upgrade python to 3.8
    if p.exists():
        p.unlink()


def createDS(ds_name, ds_format, geom_type, srs):
    """Create a new OGR Dataset

    Arguments:
        ds_name {Path} -- Location to store the DS
        ds_format {str} -- Type of DS being made (e.g. 'GeoJSON' or 'ESRI Shapefile')
        geom_type {unknown} -- Geometry Type, Brought in from a parent DS
        srs {unknown} -- Spatial Reference, brought in from a parent DS

    Returns:
        ogr_dataset, ogr_layer -- Brand new DS and Layer for your pleasure
    """
    # https://stackoverflow.com/questions/47038407/dissolve-overlapping-polygons-with-gdal-ogr-while-keeping-non-connected-result
    clear(ds_name)
    drv = ogr.GetDriverByName(ds_format)
    ds = drv.CreateDataSource(str(ds_name))
    lyr_name = ds_name.name
    lyr = ds.CreateLayer(lyr_name, srs, geom_type)
    return ds, lyr


def multipolygonize(geojson_path, out_path=None, remove_list=[]):
    """Turn a GeoJSON with multiple features into a GeoJSON with a single MultiPolygon feature

    Arguments:
        geojson_path {Path} -- Location of the multiple-featured GeoJSON

    Keyword Arguments:
        out_path {Path} -- Location of the GeoJSON with a single feature (default: {geojson_path.with_name(geojson_path.stem + "_multi" + geojson_path.suffix)})
        remove_list {list} -- List of feature IDs to remove from the original GeoJSON during the process (default: {[]})

    Returns:
        Path -- out_path
    """
    # https://stackoverflow.com/questions/47038407/dissolve-overlapping-polygons-with-gdal-ogr-while-keeping-non-connected-result
    ds = ogr.Open(str(geojson_path))
    lyr = ds.GetLayer()
    out_path = out_path or geojson_path.with_name(
        geojson_path.stem + "_multi" + geojson_path.suffix
    )
    out_ds, out_lyr = createDS(
        out_path, ds.GetDriver().GetName(), lyr.GetGeomType(), lyr.GetSpatialRef(),
    )
    defn = out_lyr.GetLayerDefn()
    multi = ogr.Geometry(ogr.wkbMultiPolygon)
    lyr.ResetReading()
    for feat in lyr:
        if feat.geometry() and (feat.GetFID() not in remove_list):
            feat.geometry().CloseRings()  # this copies the first point to the end
            geom = feat.geometry()
            if geom.GetGeometryType() == 6:  # The geometry is already multipolygon
                multi = geom
            elif (
                geom.GetGeometryType() == 2
            ):  # The geometry is a linestring (ignore it)
                pass
            else:
                wkt = geom.ExportToWkt()
                new_geo = ogr.CreateGeometryFromWkt(wkt)
                new_geo = new_geo.Buffer(0)
                multi.AddGeometryDirectly(new_geo)
    execute_union = (lyr.GetFeatureCount() > 1) and (
        lyr.GetFeatureCount() > len(set(remove_list))
    )

    union = multi.UnionCascaded() if execute_union else multi

    out_feat = ogr.Feature(defn)
    out_feat.SetGeometry(union)
    out_lyr.CreateFeature(out_feat)
    del out_ds
    del ds
    return out_path


def unify(json_paths, out_path=None):
    """Merge multiple GeoJsons with overlapping polygons into a single GeoJSON with "union" applied to all overlapping features

    Arguments:
        json_paths {list of Paths} -- Multi-featured GeoJSONs to be merged
        out_path {Path} -- Location where monolithic result of UNION will be stored

    Returns:
        Path -- out_Path
    """
    if server_config.VERBOSE:
        print("Combining all model outputs")
    out_path = out_path or json_paths[0].with_name("_union.geojson")
    joined_paths = "' '".join([str(g) for g in json_paths])
    fc_name = "merged_features"

    # Merge all the GeoJSONs into an intermediate file, where overlapping polygons are not UNIONed
    cmd = f"ogrmerge.py -o '{out_path}' '{joined_paths}' -single -f GeoJSON -overwrite_ds -nln '{fc_name}'"
    run(cmd, stdout=PIPE, shell=True)

    # Dissolve overlapping polygons into each other
    cmd = f"ogr2ogr {out_path} {out_path} -dialect sqlite -sql 'SELECT ST_Union(geometry), * FROM {fc_name}'"
    run(cmd, stdout=PIPE, shell=True)
    if server_config.VERBOSE:
        print("^^^ IGNORE THIS ERROR ^^^")  # XXXJona, why does this error occur?

    return out_path


def sparsify(superset_path, subset_paths, out_path=None):
    """Helper function to discard polygons from a superset that do not intersect at least one geometry from each subset geojson

    Arguments:
        superset_path {Path} -- Location of the superset GeoJSON that is overlaps the subset GeoJSONs
        subset_paths {List of Paths} -- List of Locations of the small disparate GeoJSONs

    Keyword Arguments:
        out_path {Path} -- Location to store a new GeoJSON containing only the overlapping elements from the superset (default: {superset_path.with_name("slick.geojson")})

    Returns:
        Path -- out_path
    """
    # Function to delete features that don't intersect another file
    if server_config.VERBOSE:
        print("Discarding extraneous polygons")

    out_path = out_path or superset_path.with_name("slick.geojson")

    superset = ogr.Open(str(superset_path))
    super_lay = superset.GetLayer(0)
    remove_polys = []

    for geojson in subset_paths:
        multi_geo = multipolygonize(geojson)
        multi = ogr.Open(str(multi_geo))
        multi_lay = multi.GetLayer(0)
        multi_feat = multi_lay.GetFeature(0)
        multi_geom = multi_feat.geometry()
        empty = multi_geom.IsEmpty()

        super_lay.ResetReading()
        for super_feat in super_lay:
            super_geom = super_feat.geometry()
            if empty:  # Can't run Intersects on an empty geometry, so check first
                remove_polys += [super_feat.GetFID()]
            else:
                if not super_geom.Intersects(multi_geom):
                    remove_polys += [super_feat.GetFID()]
        del multi
        clear(multi_geo)
    del superset

    clear(out_path)
    multipolygonize(superset_path, out_path, remove_list=remove_polys)

    clear(superset_path)
    return out_path


def intersection(multi_1_path, multi_2_path, out_path=None):
    """Function to clip the intersection between two geojsons

    Arguments:
        out_path {Path} -- Location of the output
        multi_1_path {Path} -- Location of one of the GeoJSON
        multi_2_path {Path} -- Location of the second GeoJSON

    Returns:
        Path -- The location of the output GeoJSON
    """
    if server_config.VERBOSE:
        print("Trimming to finest resolution")
    out_path = out_path or multi_1_path.with_name("clipped.geojson")
    multi_1_path = multipolygonize(multi_1_path)
    multi_2_path = multipolygonize(multi_2_path)
    cmd = f"ogr2ogr '{out_path}' '{multi_1_path}' -clipsrc '{multi_2_path}' -explodecollections"
    run(cmd, stdout=PIPE, shell=True)
    multipolygonize(
        out_path, out_path=out_path
    )  # This is used to eliminate any extraneous linestrings that are generated
    clear(multi_1_path)
    clear(multi_2_path)
    return out_path


def multi_machine(
    pid,
    pkls,
    thresholds=ml_config.THRESH,
    grd_path=None,
    out_path=None,
    fine_pkl_idx=-1,
):
    """Run inference on a GRD using multiple ML PKLs, and combine them to get a single multipolygon

    Arguments:
        pid {str} -- Sentinel 1 product ID, e.g. 'S1A_IW_GRDH_1SDV_20200406T194140_20200406T194205_032011_03B2AB_C112'
        pkls {list of strs} -- list of model pkls to run on indicated product e.g. ["2_18_128_0.676.pkl", "2_18_256_0.691.pkl", "2_18_512_0.705.pkl"]

    Keyword Arguments:
        thresholds {int or list of ints} -- Thresholds to be applied to each ML PKL respectively (default: {ml_config.THRESH})
        grd_path {Path} -- Location of GRD tiff  (default: {grd_path = Path(path_config.LOCAL_DIR) / "temp" / pid / "vv_grd.tiff"})
        out_path {Path} -- Location where final GeoJSON should be saved (default: {grd_path.with_name(f"slick_{'-'.join([str(t) for t in thresholds])}conf.geojson")})
        fine_pkl_idx {int} -- Which PKL gives the finest resolution result, will be used to trim output (default: {-1})

    Returns:
        Path -- out_path
    """
    if grd_path:
        path = grd_path.parent
    else:
        path = Path(path_config.LOCAL_DIR) / "temp" / pid
        grd_path = path / "vv_grd.tiff"

    if not isinstance(thresholds, list):
        thresholds = [thresholds] * len(pkls)

    out_path = out_path or grd_path.with_name(
        f"slick_{'-'.join([str(t) for t in thresholds])}conf.geojson"
    )

    if not grd_path.exists():  # pylint: disable=no-member
        sho = SHO(pid)
        sho.download_grd_tiff_from_s3(grd_path)

    #  Run machine learning on vv_grd to make 3 contours
    geojson_paths = []
    for i, pkl in enumerate(pkls):
        if server_config.VERBOSE:
            print("Running Inference on", pkl)
        geojson_path = (path / pkl).with_suffix(".geojson")
        inference_path = (path / pkl).with_suffix(".tiff")
        intermediary_path = path / "intermediary.tiff"
        clear(geojson_path)

        if not inference_path.exists():
            machine(load_learner_from_s3(pkl, False), grd_path, inf_path=inference_path)

        cmd = f"gdal_calc.py -A '{inference_path}' --outfile='{intermediary_path}' --calc='1*(A>={thresholds[i]})' --NoDataValue=0"
        run(cmd, stdout=PIPE, shell=True)
        cmd = f"gdal_polygonize.py '{intermediary_path}' -f GeoJSON '{geojson_path}'"
        run(cmd, stdout=None if server_config.VERBOSE else PIPE, shell=True)

        clear(intermediary_path)
        geojson_paths += [geojson_path]

    union_path = unify(geojson_paths)
    coarse_path = sparsify(union_path, geojson_paths)
    out_path = intersection(geojson_paths[fine_pkl_idx], coarse_path, out_path)
    clear(coarse_path)
    return out_path


# %%
