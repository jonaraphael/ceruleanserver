#%%
from abc import ABC, abstractmethod
from db_connection import DBConnection
from shapely.geometry import MultiPolygon, shape
from shapely import wkb

db = DBConnection()


class ORM_Base(ABC):
    id = None

    @property
    @abstractmethod
    def name(self):
        return "table name"

    @property
    @abstractmethod
    def pk(self):
        return "primarykey name"

    @property
    def fields(self):
        # XXX Need to handle case of empty self.fields in all scenarios throughout the code!
        return self._fields

    @fields.setter  # XXX This is not working??? The setter never gets called when changing dict values :(
    def fields(self, x):
        if self.id is not None:
            raise Exception()  # Cannot update an object loaded from the database!
        else:
            self._fields = x

    def write(self):
        if self.id is None:
            res = db.insert_dict_as_row(self.fields, self.name)
            if res:
                self.id = res
        else:
            raise Exception()  # Can't edit an existing row

    def __init__(self, id=None, **kwargs):
        if id:
            self.fields = db.read_field_from_field_value_table(
                "*", self.pk, id, self.name
            ).iloc[0]
            self.id = id
        else:
            if not all([f in self.fields for f in kwargs]):
                raise Exception()  # Unaccepted fields supplied
            self.fields = (
                db.load_schema(self.name).append(kwargs, ignore_index=True).iloc[0]
            )

    def __repr__(self):
        return f"<{self.name} '{self.id}': {self.fields}>"


class Vessel(ORM_Base):
    name = "vessel"
    pk = "mmsi"

    def get_coincidents(self):
        res = db.read_field_from_field_value_table(
            "coincident_id", "vessel_mmsi", self.fields["mmsi"], "coincident"
        )
        return [Coincident(id=r) for r in res]


class Coincident(ORM_Base):
    name = "coincident"
    pk = "coincident_id"

    def geometry(self):
        return wkb.loads(self.fields["geometry"], hex=True)

    def get_vessel(self):
        return Vessel(id=self.fields["vessel_mmsi"])

    def shareable(self):
        if self.id is not None:
            v = self.get_vessel()
            joined = self.fields.append(v.fields)
            return joined
        else:
            raise Exception()  # Not yet stored in the DB


class Posi_Poly(ORM_Base):
    name = "posi_poly"
    pk = "posi_poly_id"

    def geometry(self):
        return wkb.loads(self.fields["geometry"], hex=True)

    def get_slick(self):
        return Slick(id=self.fields["slick_id"])

    def get_inference(self):
        return Inference(id=self.fields["inference_id"])

    def get_eezs(self):
        res = db.read_field_from_field_value_table(
            "eez_mrgid", "posi_poly_id", self.fields["posi_poly_id"], "posi_poly_eez"
        )
        return [Eez(id=r) for r in res]

    def get_coincidents(self):
        res = db.read_field_from_field_value_table(
            "coincident_id", "posi_poly_id", self.fields["posi_poly_id"], "coincident"
        )
        return [Coincident(id=r) for r in res]


class Slick(ORM_Base):
    name = "slick"
    pk = "slick_id"

    def geometry(self):
        return MultiPolygon([p.geometry() for p in self.get_posi_polys()])

    def get_posi_polys(self):
        return [Posi_Poly(id=i) for i in self.fields["posi_poly_ids"]]

    def to_dict(self):
        if self.id is not None:
            polys = self.get_posi_polys()
            res = {
                "slick_id": self.fields["slick_id"],
                "geometry": self.geometry(),
                "eezs": set(
                    [e.fields["geoname"] for e in p.get_eezs() for p in polys]
                ),  # These two nested list comprehension need some help
                "coincidents": set(
                    [c.to_dict() for c in p.get_coincidents() for p in polys]
                ),  # These two nested list comprehension need some help
                "timestamp": polys[0].get_inference().fields["starttime"],
            }
            return res
        else:
            raise Exception()  # Not yet stored in the DB


class Eez(ORM_Base):
    name = "eez"
    pk = "mrgid"

    def geometry(self):
        return wkb.loads(self.fields["geometry"], hex=True)

    def get_posi_polys(self):
        res = db.read_field_from_field_value_table(
            "posi_poly_id", "eez_mrgid", self.fields["mrgid"], "posi_poly_eez"
        )
        return [Posi_Poly(id=r) for r in res]


class Inference(ORM_Base):
    name = "inference"
    pk = "mrgid"

    def geometry(self):
        return MultiPolygon([p.geometry() for p in self.get_posi_polys()])

    def get_posi_polys(self):
        res = db.read_field_from_field_value_table(
            "posi_poly_id", "inference_id", self.fields["inference_id"], "posi_poly"
        )
        return [Posi_Poly(id=r) for r in res]

    def get_sns(self):
        return Sns(id=self.fields["sns_grd_id"])

    def get_ocn(self):
        return Ocn(id=self.fields["ocn_ocn_id"])

    def get_models(self):
        return [Model(id=i) for i in self.fields["model_ids"]]


class Model(ORM_Base):
    name = "model"
    pk = "model_id"

    def get_inferences(self):
        res = db.read_field_from_field_value_table(
            "inference_id", "model_ids", self.fields["model_id"], "inference"
        )  # This is wrong -- want to do an WHERE IN clause instead
        return [Inference(id=r) for r in res]


class Ocn(ORM_Base):
    name = "ocn"
    pk = "ocn_id"

    def get_sns(self):
        return Sns(id=self.fields["sns_grd_id"])


class Sns(ORM_Base):
    name = "sns"
    pk = "grd_id"

    def get_inferences(self):
        res = db.read_field_from_field_value_table(
            "inference_id", "sns_grd_id", self.fields["grd_id"], "inference"
        )
        return [Inference(id=r) for r in res]

    def get_ocns(self):
        res = db.read_field_from_field_value_table(
            "ocn_id", "sns_grd_id", self.fields["grd_id"], "ocn"
        )
        return [Ocn(id=r) for r in res]


# %%
