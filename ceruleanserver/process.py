from data_objects import Sns_Ext, Grd_Ext, SHO, Inference_Ext, Posi_Poly_Ext
from configs import server_config
from ml.inference import run_inference
from alchemy import commit


def process_sns(raw):
    """Processes the raw SNS received from Sinergise
    
    Arguments:  
        raw {dict} -- contains all the metadata and details of a new satellite image on S3
    """

    sns = Sns_Ext(raw)  ### Move this to the Lambda Function
    if sns.db_base.id and server_config.BLOCK_REPEAT_SNS:
        return  # The SNS exists in our table already but we don't want to reprocess it

    grd = Grd_Ext(sns)
    ocn = None
    # ocn = SHO(grd).ocn  
    # XXX Not all GRDs sent via SNS exist on SciHub?!
    # XXX There can be multiple GRDs per OCN e.g. 
    # S1A_IW_OCN__2SDV_20200720T165734_20200720T165759_033541_03E2F9_4FA6
    # maps to
    # S1A_IW_GRDH_1SDV_20200720T165734_20200720T165759_033541_03E2F9_492B and
    # S1A_IW_GRDH_1SDV_20200720T165734_20200720T165759_033541_03E2F9_C621
    
    if server_config.DOWNLOAD_GRDS:
        grd.download_grd_tiff()  # Download Large GeoTiff

    if grd.file_path.exists() and server_config.RUN_ML:
        inf = Inference_Ext(grd, ocn)  # Instantiates an empty Inference_Ext object
        run_inference(inf)  # Fills in empty attributes of inf

        inf.posi_polys = [Posi_Poly_Ext(inf, poly) for poly in inf.polys]

        if inf.posi_polys and server_config.VERBOSE:
            print(len(inf.posi_polys), "polygons found on ", grd.pid)

        if inf.polys and server_config.UPLOAD_OUTPUTS:
            inf.save_small_to_s3()
            inf.save_poly_to_s3()
    if grd.is_downloaded and server_config.CLEANUP_SNS:
        grd.cleanup()

    commit()
