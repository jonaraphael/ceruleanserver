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
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from configs import server_config

Base = declarative_base()


class SmartBase(Base):
    __abstract__ = True
    __tablename__ = "NO_TABLE_NAME_SET"

    loaded_from_db = False

    # XXXHELP Why doesn't the load pathway work??!
    def load_or_insert(self, sess, filter_attrs=[]):
        """ Looks for a single row in the DB that matches kwargs, returns it or creates a new row
        Returns:
            Alchemy Instance: An instance of the class specified
        """
        self.load(sess, filter_attrs=filter_attrs)
        sess.add(self)

    def load(self, sess, filter_attrs=[]):
        """ Gets an object from a table in the db
        """
        filters = {attr: getattr(self, attr) for attr in filter_attrs}
        q = sess.query(self.__class__).filter_by(**filters)
        instance = q.one_or_none()
        if instance:
            self.__dict__.update(instance.__dict__)
            self.loaded_from_db = True
            sess.expire(instance)

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
    geometry = Column(Geometry())

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

    grd = relationship(
        "Grd",
        back_populates="sns",
        uselist=False,
        enable_typechecks=False,  # XXXHELP How can I do this without disabling typechecks?
        cascade_backrefs=False,
    )


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

    sns = relationship(
        "Sns",
        back_populates="grd",
        foreign_keys=sns__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    ocn = relationship(
        "Ocn",
        back_populates="grd",
        uselist=False,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    inferences = relationship(
        "Inference",
        back_populates="grd",
        enable_typechecks=False,
        cascade_backrefs=False,
    )


class Ocn(SmartBase):
    __tablename__ = "ocn"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    grd__id = Column(Integer, ForeignKey("grd.id"))
    pid = Column(String, unique=True)
    uuid = Column(postgresql.UUID)
    summary = Column(String)
    producttype = Column(String)
    filename = Column(String)

    grd = relationship(
        "Grd",
        back_populates="ocn",
        foreign_keys=grd__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    inferences = relationship(
        "Inference",
        back_populates="ocn",
        enable_typechecks=False,
        cascade_backrefs=False,
    )


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

    grd = relationship(
        "Grd",
        back_populates="inferences",
        foreign_keys=grd__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    ocn = relationship(
        "Ocn",
        back_populates="inferences",
        foreign_keys=ocn__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    posi_polys = relationship(
        "Posi_Poly",
        back_populates="inference",
        enable_typechecks=False,
        cascade_backrefs=False,
    )


class Posi_Poly(SmartGeo):
    __tablename__ = "posi_poly"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    inference__id = Column(Integer, ForeignKey("inference.id"))
    slick__id = Column(Integer, ForeignKey("slick.id"))

    inference = relationship(
        "Inference",
        back_populates="posi_polys",
        foreign_keys=inference__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    slick = relationship(
        "Slick",
        back_populates="posi_polys",
        foreign_keys=slick__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    coincidents = relationship(
        "Coincident",
        back_populates="posi_poly",
        enable_typechecks=False,
        cascade_backrefs=False,
    )


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

    coincidents = relationship(
        "Coincident",
        back_populates="vessel",
        enable_typechecks=False,
        cascade_backrefs=False,
    )


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

    posi_poly = relationship(
        "Posi_Poly",
        back_populates="coincidents",
        foreign_keys=posi_poly__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )
    vessel = relationship(
        "Vessel",
        back_populates="coincidents",
        foreign_keys=vessel__id,
        enable_typechecks=False,
        cascade_backrefs=False,
    )


class Slick(SmartBase):
    __tablename__ = "slick"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)

    posi_polys = relationship(
        "Posi_Poly",
        back_populates="slick",
        enable_typechecks=False,
        cascade_backrefs=False,
    )


class Eez(SmartGeo):
    __tablename__ = "eez"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    mrgid = Column(Integer)
    geoname = Column(String)
    pol_type = Column(String)
    sovereigns = Column(ARRAY(String))
