from data_objects import Sns_Ext, Grd_Ext, SHO, Inference_Ext, Posi_Poly_Ext, Slick_Ext
from configs import server_config
from ml.inference import run_inference
from db_connection import session_scope


def process_sns(raw):
    """Processes the raw SNS received from Sinergise
    
    Arguments:  
        raw {dict} -- contains all the metadata and details of a new satellite image on S3
    """
    with session_scope() as sess:
        
        sns = Sns_Ext(raw) ### Move this to the Lambda Function
        sess.add(sns)
        
        if sns.loaded_from_db and server_config.BLOCK_REPEAT_SNS:
            return  # The SNS exists in our table already but we don't want to reprocess it

        grd = Grd_Ext(sns)
        sess.add(grd)
        # ocn = SHO(grd).ocn
        # sess.add(ocn)
        # XXX Not all GRDs sent via SNS exist on SciHub?!
        # XXX There can be multiple GRDs per OCN e.g. 
        # S1A_IW_OCN__2SDV_20200720T165734_20200720T165759_033541_03E2F9_4FA6
        # maps to
        # S1A_IW_GRDH_1SDV_20200720T165734_20200720T165759_033541_03E2F9_492B and
        # S1A_IW_GRDH_1SDV_20200720T165734_20200720T165759_033541_03E2F9_C621
        
        if server_config.DOWNLOAD_GRDS:
            grd.download_grd_tiff()  # Download Large GeoTiff

        if grd.file_path.exists() and server_config.RUN_ML:
            inf = Inference_Ext(grd) # Instantiates an empty Inference_Ext object
            run_inference(inf)  # Fills in empty attributes of inf
            sess.add(inf)

            for poly in inf.polys:
                posi_poly = Posi_Poly_Ext(inf, poly)
                sess.add(posi_poly)

            if inf.posi_polys:
                slick = Slick_Ext(inf.posi_polys)
                sess.add(slick)

                if server_config.VERBOSE:
                    print(len(inf.posi_polys), "polygons found on ", grd.pid)

                if server_config.UPLOAD_OUTPUTS:
                    inf.save_small_to_s3()
                    inf.save_poly_to_s3()
        if grd.is_downloaded and server_config.CLEANUP_SNS:
            grd.cleanup()
