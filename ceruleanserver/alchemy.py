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
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import InvalidRequestError, IntegrityError
from geoalchemy2 import Geometry
from configs import server_config
from db_connection import DBConnection

Base = declarative_base()
db = DBConnection()  # Database Object


def get_or_create(model, **kwargs):
    instance = (
        db.sess.query(model).filter_by(**kwargs).first()
    )  # XXX Modify this to focus on unique columns?
    if not instance:
        instance = model(**kwargs)
        db.sess.add(
            instance
        )  # This leaves ID blank, whereas successful query fills the id
    return instance


def commit():
    try:
        db.sess.commit()
    except (InvalidRequestError, IntegrityError) as e:
        print(e)
        db.sess.rollback()
        print("Rolled Back")


class Base_Geo(Base):
    __abstract__ = True
    geometry = Column(Geometry())


class Sns(Base):
    __tablename__ = "sns"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    messageid = Column(postgresql.UUID, unique=True)
    subject = Column(String)
    timestamp = Column(DateTime)

    grd = relationship("Grd", back_populates="sns", uselist=False)

    def __repr__(self):
        return f"<{self.__tablename__} {self.id}: {self.subject, self.timestamp, self.messageid}>"


class Grd(Base_Geo):
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

    sns = relationship("Sns", back_populates="grd", foreign_keys=sns__id)
    ocn = relationship("Ocn", back_populates="grd", uselist=False)
    inferences = relationship("Inference", back_populates="grd")

    def __repr__(self):
        return f"<{self.__tablename__} {self.id}: {self.pid, self.starttime, self.mode, self.polarization}>"


class Ocn(Base):
    __tablename__ = "ocn"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    grd__id = Column(Integer, ForeignKey("grd.id"))
    pid = Column(String, unique=True)
    uuid = Column(postgresql.UUID)
    summary = Column(String)
    producttype = Column(String)
    filename = Column(String)

    grd = relationship("Grd", back_populates="ocn", foreign_keys=grd__id)
    inferences = relationship("Inference", back_populates="ocn")

    def __repr__(self):
        return f"<{self.__tablename__} {self.id}: {self.pid, self.grd__id, self.summary, self.producttype}>"


class Inference(Base):
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

    grd = relationship("Grd", back_populates="inferences", foreign_keys=grd__id)
    ocn = relationship("Ocn", back_populates="inferences", foreign_keys=ocn__id)
    posi_polys = relationship("Posi_Poly", back_populates="inference")

    def __repr__(self):
        return f"<{self.__tablename__} {self.id}: {self.grd__id, self.thresholds}>"


class Posi_Poly(Base_Geo):
    __tablename__ = "posi_poly"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    inference__id = Column(Integer, ForeignKey("inference.id"))

    inference = relationship(
        "Inference", back_populates="posi_polys", foreign_keys=inference__id
    )
    coincidents = relationship("Coincident", back_populates="posi_poly")

    def __repr__(self):
        return f"<{self.__tablename__} {self.id}: {self.inference__id}>"


class Vessel(Base):
    __tablename__ = "vessel"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    mmsi = Column(Integer, unique=True)
    name = Column(String)
    flag = Column(String)
    callsign = Column(String)
    imo = Column(String)
    shiptype = Column(String)
    length = Column(Numeric)

    coincidents = relationship("Coincident", back_populates="vessel")

    def __repr__(self):
        return f"<{self.__tablename__} {self.id}: {self.inference__id}>"


class Coincident(Base_Geo):
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
        "Posi_Poly", back_populates="coincidents", foreign_keys=posi_poly__id
    )
    vessel = relationship(
        "Vessel", back_populates="coincidents", foreign_keys=vessel__id
    )

    def __repr__(self):
        return f"<{self.__tablename__} {self.id}: {self.posi_poly__id} {self.vessel__id} {self.score} {self.method}>"


class Slick(Base):
    __tablename__ = "slick"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    posi_poly__id = Column(ARRAY(Integer))

    posi_polys = "XXX TO BE IMPLEMENTED"  #

    def __repr__(self):
        return f"<{self.__tablename__} {self.id}: {self.posi_poly__id}>"


class Eez(Base_Geo):
    __tablename__ = "eez"
    id = Column(Integer, Sequence(f"{__tablename__}_id_seq"), primary_key=True)
    mrgid = Column(Integer)
    geoname = Column(String)
    pol_type = Column(String)
    sovereigns = Column(ARRAY(String))

    def __repr__(self):
        return f"<{self.__tablename__} {self.id}: {self.mrgid, self.geoname, self.pol_type, self.sovereigns}>"
