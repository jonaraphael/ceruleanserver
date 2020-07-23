from sqlalchemy import create_engine, Table, Column, Integer, Sequence, String, ForeignKey, ARRAY, JSON, DATETIME
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver"))
sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver" / "ml"))
sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver" / "utils"))
from configs import testing_config, path_config, server_config  # pylint: disable=import-error
from alchemy import Grd
 
engine = create_engine(f'postgresql://{server_config.DB_USER}:{server_config.DB_PASSWORD}@{server_config.DB_HOST}:{server_config.DB_PORT}/{server_config.DB_DATABASE}?gssencmode=disable', echo=True)
Session = sessionmaker(bind=engine)
session = Session()
 
example = session.query(Grd).first()
print(example)
 

# a = session.query(Grd).first()
# display(a.sns)

# example = session.query(Eez).filter(Eez.mrgid == 8389).order_by(Eez.mrgid).all()

# test_model = Model(name="test", date="2020-06-01")
# session.add(test_model)
# session.commit()

# one to one https://www.youtube.com/watch?v=JI76IvF9Lwg
# many to one https://www.youtube.com/watch?v=juPQ04_twtA
# many to many https://www.youtube.com/watch?v=OvhoYbjtiKc

# https://docs.sqlalchemy.org/en/13/orm/basic_relationships.html#relationships-one-to-one
# https://docs.sqlalchemy.org/en/13/orm/tutorial.html#common-filter-operators
