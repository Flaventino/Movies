from sqlalchemy import create_engine, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy import Column, ForeignKey, Integer, String, Date, Numeric
from sqlalchemy.orm import sessionmaker, declarative_base, relationship

# INSTANCIATING A DATABASE FRAMEWORK (i.e. a mix of container and base class) 
MovieDB = declarative_base()

# HELPER FUNCTIONS (To simplify the use or implementation of the database)
def foreign_key(target):
    """
    Returns a python list to use in `column` function call.

    This function helps to avoid redundancy when defining a foreign key.
    It is indeed not anymore required to explicitely indicates the data type.
    Before: `newColumn = Column(<data type>, ForeignKey(target))`
    Now   : `NewColumn = Column(*foreign_key(target))`

    Parameter(s):
        target (str): Full path to the target column
                      Ex: 'target_table_name.target_column_name'
    """

    # EXTRACTS THE TABLE AND COLUMN NAMES FROM THE 'target' ARGUMENT
    table_name, column_name = [item.strip() for item in target.split('.')]
    
    # RETRIEVES THE METADATA ABOUT THE REFERENCED COLUMN
    target_column = MovieDB.metadata.tables[table_name].columns[column_name]
    
    # RETURNS A LIST TO BE USED AS ARGUMENT IN ANY `Column` METHOD CALL
    return [target_column.type, ForeignKey(target)]

def db_connect(url: str = "sqlite:///./movies.db", **kwargs):
    """
    Creates or updates the database and returns access to it.

    The database itself is never overwritten but only udpdated or created when
    not already in place. The same for the tables inside the database.

    Parameter(s):
        url (str): url to connect the choosen database.
                   By default: "sqlite:///../movies.db"
        **kwargs : Additional arguments to be passed to `create_engine` method.
                   For any details about the said method, see SQLAlchemy doc.

    Returns:
        A SQLAlchemy `sessionmaker` object to be instanciated into sessions.
    """
    # SET THE DB TYPE (sqlite, PostgreSQL, etc.) AND AN ENGINE (i.e. connector)
    engine = create_engine(url, **kwargs)

    # CREATES THE REQUIRED DATABASE (according data given into `url`)
    MovieDB.metadata.create_all(engine)

    # FUNCTION OUTPUT (returns a `sessionmaker` object to help DB interactions)
    return sessionmaker(bind=engine)

# CREATING TABLES OF THE DATABASE
class Movies(MovieDB):
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'movies'

    # SPECIFIC TABLE COLUMNS
    # 1. Main movie characteritics
    Id = Column(Integer, primary_key=True, autoincrement=True)
    Title = Column(String, nullable=True)
    Title_Fr = Column(String, nullable=False)
    Synopsis = Column(String, nullable=True)
    Duration = Column(Integer, nullable=True)
    Poster_URL = Column(String, nullable=True)
    Press_Rating = Column(Numeric(precision=2, scale=1), nullable=True)
    Public_Rating = Column(Numeric(precision=2, scale=1), nullable=True)

    # 2. Tecnical details
    Visa = Column(String, nullable=True)
    Awards = Column(String, nullable=True)
    Budget = Column(String, nullable=True)
    Format = Column(String, nullable=True)            # Whether color or B&W
    Category = Column(String, nullable=True)          # Feature film, doc, etc.
    Release_Date = Column(Date, nullable=True)
    Release_Place = Column(String, nullable=True)
    Production_Year = Column(Integer, nullable=True)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy model and use)
    genres = relationship('Genres', back_populates='movies')
    actors = relationship('Actors', back_populates='movies')
    languages = relationship('Languages', back_populates='movies')
    directors = relationship('Directors', back_populates='movies')
    distributors = relationship('Distributors', back_populates='movies')
    screenwriters = relationship('ScreenWriters', back_populates='movies')
    nationalities = relationship('Nationalities', back_populates='movies')

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (UniqueConstraint(*('Title_Fr', 'Release_Date'),
                                       name='Title_&_date_is_a_unique_pair'),)

class Persons(MovieDB):
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'persons'

    # SPECIFIC TABLE COLUMNS
    Id = Column(Integer, primary_key=True, autoincrement=True)
    Full_Name = Column(String, nullable=False)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    actor_play = relationship('Actors', back_populates='persons')
    film_making = relationship('Directors', back_populates='persons')
    screeplay_writing = relationship('ScreenWriters', back_populates='persons')

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (UniqueConstraint('Full_Name',
                                       name='Full_Name_should_be_unique'),)

class PeopleRole(MovieDB): # Abstract table for code factorization purpose)
    """
    Helps factorizing code as it is a common part of other sub classes
    """
    # RAW PARAMETERS AND SETINGS
    __abstract__ = True

    # COLUMNS OF THE ABSTRACT TABLE
    MovieId = Column(*foreign_key('movies.Id'))
    PersonId = Column(*foreign_key('persons.Id'))

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (PrimaryKeyConstraint(*('MovieId', 'PersonId'),
                                           name='Composite_primary_key'),)

class Actors(PeopleRole):
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'actors'

    # SPECIFIC TABLE COLUMNS
    Characters = Column(String, nullable=False)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='actors')
    persons = relationship('Persons', back_populates='actor_play')

class Directors(PeopleRole):
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'directors'

    # SPECIFIC TABLE COLUMNS (none at this stage)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='directors')
    persons = relationship('Persons', back_populates='film_making')

class ScreenWriters(PeopleRole):
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'screenwriters'

    # SPECIFIC TABLE COLUMNS (none at this stage)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='screenwriters')
    persons = relationship('Persons', back_populates='screeplay_writing')

class Companies(MovieDB): 
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'companies'

    # SPECIFIC TABLE COLUMNS
    Id = Column(Integer, primary_key=True, autoincrement=True)
    Full_Name = Column(String, nullable=False)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    distribution = relationship('Distributors', back_populates='companies')

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (UniqueConstraint('Full_Name',
                                       name='Full_Name_should_be_unique'),)

class Distributors(MovieDB): 
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'distributors'

    # SPECIFIC TABLE COLUMNS
    MovieId = Column(*foreign_key('movies.Id'))
    CompId = Column(*foreign_key('companies.Id'))

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (PrimaryKeyConstraint(*('MovieId', 'CompId'),
                                           name='Composite_primary_key'),)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='distributors')
    companies = relationship('Companies', back_populates='distribution')

class Genres(MovieDB): 
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'genres'

    # SPECIFIC TABLE COLUMNS
    MovieId = Column(*foreign_key('movies.Id'))
    Genre = Column(String, nullable=False)

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (PrimaryKeyConstraint(*('MovieId', 'Genre'),
                                           name='Composite_primary_key'),)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='genres')

class Nationalities(MovieDB): 
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'nationalities'

    # SPECIFIC TABLE COLUMNS
    MovieId = Column(*foreign_key('movies.Id'))
    Nationality = Column(String, nullable=False)

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (PrimaryKeyConstraint(*('MovieId', 'Nationality'),
                                           name='Composite_primary_key'),)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='nationalities')

class Languages(MovieDB): 
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'languages'

    # SPECIFIC TABLE COLUMNS
    MovieId = Column(*foreign_key('movies.Id'))
    Language = Column(String, nullable=False)

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (PrimaryKeyConstraint(*('MovieId', 'Language'),
                                           name='Composite_primary_key'),)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='languages')