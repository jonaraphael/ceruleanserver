from sqlalchemy import (
    Column,
    Integer,
    Sequence,
    String,
    ForeignKey,
    ARRAY,
    JSON,
    DateTime,
    Boolean,
    Numeric,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import reconstructor
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geography
from configs import server_config

Base = declarative_base()

# TODO remove session knowledge from bases. Instead, create other functions to accomplish that

class SmartBase(Base):
    __abstract__ = True
    __tablename__ = "NO_TABLE_NAME_SET"

    loaded_from_db = False
    
    @reconstructor # This code will be run once when this object is loaded from the DB. It cannot accept parameters
    def init_on_load(self):
        self.loaded_from_db = True

    # XXXHELP Why doesn't the load pathway work??!
    def load_or_insert(self, sess, filter_attrs=[]):
        """ Looks for a single row in the DB that matches kwargs, returns it or creates a new row
        Returns:
            Alchemy Instance: An instance of the class specified
        """
        loaded = self.load(sess, filter_attrs=filter_attrs)
        if loaded:
            return loaded
        else:
            sess.add(self)
            return self

    def load(self, sess, filter_attrs=[]):
        """ Gets an object from a table in the db
        """
        filters = {attr: getattr(self, attr) for attr in filter_attrs}
        q = sess.query(self.__class__).filter_by(**filters)
        instance = q.one_or_none()
        if instance:
            for key, value in self.__dict__.iteritems():
                setattr(instance, key, value)
            instance.loaded_from_db = True
            return instance
        else:
            return None

    def __repr__(self):
        return f"<{self.__tablename__} {self.id}: {self.loaded_from_db}>"

    @classmethod
    def from_id(cls, id, sess):
        obj = sess.query(cls).get(id)
        if obj:
            obj.loaded_from_db = True
        return obj


class SmartGeo(SmartBase):
    __abstract__ = True
    geometry = Column(Geography())

    def get_intersecting_objects(self, sess, target_cls):
        return sess.query(target_cls).filter(
            target_cls.geometry.ST_Intersects(self.geometry)
        )


class Sns(SmartBase):
    __tablename__ = "sns"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    messageid = Column(postgresql.UUID, unique=True)
    subject = Column(String)
    timestamp = Column(DateTime)


class Grd(SmartGeo):
    __tablename__ = "grd"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    sns__id = Column(Integer, ForeignKey("sns.id"))
    pid = Column(String, unique=True)
    uuid = Column(postgresql.UUID)
    absoluteorbitnumber = Column(Integer)
    mode = Column(String)
    polarization = Column(String)
    s3ingestion = Column(DateTime)
    scihubingestion = Column(DateTime)
    starttime = Column(DateTime)
    stoptime = Column(DateTime)
    

class Ocn(SmartBase):
    __tablename__ = "ocn"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    grd__id = Column(Integer, ForeignKey("grd.id"))
    pid = Column(String, unique=True)
    uuid = Column(postgresql.UUID)
    summary = Column(String)
    producttype = Column(String)
    filename = Column(String)


class Inference(SmartBase):
    __tablename__ = "inference"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    grd__id = Column(Integer, ForeignKey("grd.id"))
    ocn__id = Column(Integer, ForeignKey("ocn.id"))
    ml_pkls = Column(ARRAY(String))
    thresholds = Column(ARRAY(Integer))
    fine_pkl_idx = Column(Integer)
    chip_size_orig = Column(Integer)
    chip_size_reduced = Column(Integer)
    overhang = Column(Boolean)


class Posi_Poly(SmartGeo):
    __tablename__ = "posi_poly"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    inference__id = Column(Integer, ForeignKey("inference.id"))
    slick__id = Column(Integer, ForeignKey("slick.id"))
    class_int = Column(Integer)


class Vessel(SmartBase):
    __tablename__ = "vessel"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    mmsi = Column(Integer, unique=True)
    name = Column(String)
    flag = Column(String)
    callsign = Column(String)
    imo = Column(String)
    shiptype = Column(String)
    length = Column(Numeric)


class Coincident(SmartGeo):
    __tablename__ = "coincident"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    posi_poly__id = Column(Integer, ForeignKey("posi_poly.id"))
    vessel__id = Column(Integer, ForeignKey("vessel.id"))
    direct_hits = Column(Integer)
    proximity = Column(Numeric)
    score = Column(Numeric)
    method = Column(Numeric)
    destination = Column(String)
    speed_avg = Column(Numeric)
    status = Column(String)
    port_last = Column(String)
    port_next = Column(String)
    cargo_type = Column(String)
    cargo_amount = Column(String)


class Slick(SmartBase):
    __tablename__ = "slick"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    class_int = Column(Integer)


class Eez(SmartGeo):
    __tablename__ = "eez"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    mrgid = Column(Integer)
    geoname = Column(String)
    pol_type = Column(String)
    sovereigns = Column(ARRAY(String))
