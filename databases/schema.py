from sqlalchemy import CheckConstraint as SetRule
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# INSTANCIATING A VIRTUAL SHAPE (OR SCTRUCTURE) FOR THE DATABASE 
MovieDB = declarative_base()

# CREATING TABLES (within the database)
class movie(MovieDB):
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'movie'

    # COLUMNS OF THE TABLE
    Id = Column(Integer, primary_key=True, autoincrement=True)
    Title = Column(String, nullable=True)
    Title_Fr = Column(String, nullable=True)
    Synopsis = Column(String, nullable=True)


    # SCHEMA CONSTRAINTS
    rule1 = SetRule(name="At_least_one_title_required",
                    sqltext="title IS NOT NULL OR title_fr IS NOT NULL")

    __table_args__ = (rule1)
    pass