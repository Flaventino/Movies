from sqlalchemy import create_engine, CheckConstraint, UniqueConstraint
from sqlalchemy import Column, ForeignKey, Integer, String, Date, Numeric
from sqlalchemy.orm import sessionmaker, declarative_base, relationship

# DEFINES THE DATABASE ENVIRONMENT AND CREATES AN ENGINE (i.e. DB connector)
engine = create_engine('sqlite:///../movies.db', echo=True)

# INSTANCIATING A DATABASE FRAMEWORK (i.e. a mix of container and base class) 
MovieDB = declarative_base()

# HELPER FUNCTIONS (common to all classes and not dedicated to a specific one)
def foreign_key(target):
    """
    Returns a dictionary to use in `column` function call.

    This function helps to avoid redundancy when defining a foreign key.
    It is not anymore required to explicitely indicates the new column type.
    Before: newColumn = Column(<data type>, ForeignKey(target))
    Now:  : NewColumn = Column(**foreign_key(target))

    Parameter(s):
        target (str): Full path to the target column
                      Ex: 'target_table_name.target_column_name'
    """

    # Splits the target into table name and column name
    table_name, column_name = target.split('.')
    
    # Fetches the metadata about the referenced column
    target_column = MovieDB.metadata.tables[table_name].columns[column_name]
    
    # Returns a dictionnary to use in `Column` method calls
    return {'type_': target_column.type, 'ForeignKey': ForeignKey(target)}

# CREATING ABSTRACT TABLES
class PeopleRole(MovieDB):
    """
    Helps factorizing code as it is a common part of other sub classes
    """
    # RAW PARAMETERS AND SETINGS
    __abstract__ = True

    # COLUMNS OF THE ABSTRACT TABLE
    MovieId = Column(**foreign_key('movies.Id'))
    PersonId = Column(**foreign_key('persons.Id'))

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (UniqueConstraint(*['MovieID', 'PersonID'],
                                       name='Composite_primary_key'))

# CREATING TABLES (within the database)
class Movies(MovieDB):
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'movies'

    # COLUMNS OF THE TABLE
    # 1. Main details
    Id = Column(Integer, primary_key=True, autoincrement=True)
    Title = Column(String, nullable=True)              # See Schema constraints
    Title_Fr = Column(String, nullable=True)           # See Schema constraints
    Synopsis = Column(String, nullable=True)
    Duration = Column(Integer, nullable=True)
    Poster_url = Column(String, nullable=True)
    Press_rating = Column(Numeric(precision=2, scale=1), nullable=True)
    Public_rating = Column(Numeric(precision=2, scale=1), nullable=True)

    # 2. Tecnical details
    Visa = Column(String, nullable=True)
    Awards = Column(String, nullable=True)
    Budget = Column(String, nullable=True)
    Format = Column(String, nullable=True)            # Whether color or B&W
    Category = Column(String, nullable=True)          # Feature film, doc, etc.
    Release_date = Column(Date, nullable=True)
    Release_place = Column(String, nullable=True)
    Production_year = Column(Integer, nullable=True)
    

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (
        CheckConstraint(sqltext="title IS NOT NULL OR title_fr IS NOT NULL",
                        name="At_least_one_title_required"))

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    genres = relationship('Genres', back_populates='movies')
    actors = relationship('Actors', back_populates='movies')
    languages = relationship('Languages', back_populates='movies')
    nationalities = relationship('Nationalities', back_populates='movies')
    directors = relationship('Directors', back_populates='movies')
    screenwriters = relationship('ScreenWriters', back_populates='movies')
    distributors = relationship('Distributors', back_populates='movies')

class Persons(MovieDB):
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'persons'

    # COLUMNS OF THE TABLE
    Id = Column(Integer, primary_key=True, autoincrement=True)
    Full_name = Column(String, nullable=False)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    actor_play = relationship('Actors', back_populates='persons')
    film_making = relationship('Directors', back_populates='persons')
    screeplay_writing = relationship('ScreenWriters', back_populates='persons')

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

    # COLUMNS OF THE ABSTRACT TABLE
    Id = Column(Integer, primary_key=True, autoincrement=True)
    Full_name = Column(String, nullable=False)

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    distribution = relationship('Distributors', back_populates='companies')

class Distributors(MovieDB): 
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'distributors'

    # COLUMNS OF THE ABSTRACT TABLE
    MovieId = Column(**foreign_key('movies.Id'))
    CompId = Column(**foreign_key('distributors.Id'))

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (UniqueConstraint(*['MovieID', 'CompId'],
                                       name='Composite_primary_key'))

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='distributors')
    companies = relationship('Persons', back_populates='distribution')

class Genres(MovieDB): 
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'genres'

    # COLUMNS OF THE ABSTRACT TABLE
    MovieId = Column(**foreign_key('movies.Id'))
    genre = Column(String, nullable=False)

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (UniqueConstraint(*['MovieID', 'genre'],
                                       name='Composite_primary_key'))

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='genres')

class Nationalities(MovieDB): 
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'nationalities'

    # COLUMNS OF THE ABSTRACT TABLE
    MovieId = Column(**foreign_key('movies.Id'))
    Nationality = Column(String, nullable=False)

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (UniqueConstraint(*['MovieID', 'Nationality'],
                                       name='Composite_primary_key'))

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='nationalities')

class Languages(MovieDB): 
    # RAW PARAMETERS AND SETINGS
    __tablename__ = 'languages'

    # COLUMNS OF THE ABSTRACT TABLE
    MovieId = Column(**foreign_key('movies.Id'))
    Language = Column(String, nullable=False)

    # DEFINING SCHEMA SPECIFIC CONSTRAINTS
    __table_args__ = (UniqueConstraint(*['MovieID', 'Language'],
                                       name='Composite_primary_key'))

    # DEFINING PURE ORM RELATIONSHIPS (i.e. enhancing SQLAlchemy features)
    movies = relationship('Movies', back_populates='languages')

# PUT THE DATABASE ON HARD DRIVE AND CREATES A SESSION TO WORK WITH IT
MovieDB.metadata.create_all(engine)
session = sessionmaker(bind=engine)