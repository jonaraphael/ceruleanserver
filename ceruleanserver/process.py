from data_objects import Sns_, Grd_, SHO, Inference_, Posi_Poly_
from configs import server_config
from ml.inference import run_inference
import sqlalchemy
from alchemy import Sns # XXX Get rid of this?!

def process_sns(raw, db):
    """Processes the raw SNS received from Sinergise
    
    Arguments:  
        raw {dict} -- contains all the metadata and details of a new satellite image on S3
    """
    
    sns = Sns_(raw) ### Move this to the Lambda Function
    already_processed = db.sess.query(Sns).filter(Sns.messageid==sns.messageid).first()
    if already_processed and server_config.BLOCK_REPEAT_SNS:
        return
    db.sess.add(sns.db) ### Move this to the Lambda Function
    
    grd = Grd_(sns)
    db.sess.add(grd.db)
    
    ocn = SHO(grd).ocn # Not all GRDs sent via SNS exist on SciHub?!
    if ocn:
        db.sess.add(ocn.db)

    if server_config.DOWNLOAD_GRDS:
        grd.download_grd_tiff()  # Download Large GeoTiff
    if grd.file_path.exists() and server_config.RUN_ML:
        inf = Inference_(grd, ocn)
        run_inference(inf)
        db.sess.add(inf.db)

        posi_polys = [Posi_Poly_(inf, p).db for p in inf.polys]
        db.sess.add_all(posi_polys)
        if posi_polys and server_config.VERBOSE:
            print(len(posi_polys), "polygons found on ", grd.pid)

        if posi_polys and server_config.UPLOAD_OUTPUTS:
            inf.save_small_to_s3()
            inf.save_poly_to_s3()
    if grd.is_downloaded and server_config.CLEANUP_SNS:
        grd.cleanup()
    try:
        db.sess.commit()
    except (sqlalchemy.exc.InvalidRequestError, sqlalchemy.exc.IntegrityError) as e:
        print(e)
        db.sess.rollback()
        print("Rolled Back")
