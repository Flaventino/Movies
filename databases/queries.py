import schema
from sqlalchemy.orm import Session

def get_session(**kwargs):
    """
    Returns an auxiliary session to run queries
    """

    session = schema.db_connect(**kwargs)
    return session()

def get_persons_id(names, session=None):
    """
    Returns a dictionary with required names as keys and `Id` as values.

    Parameter(s)
        names (str|tuple|list|set): Names of people whose `Id` is requested.
                                    Either a single string or a collection of
                                    strings.
        session          (Session): OPTIONAL. SQLAlchemy session to use. If one
                                    session is provided, then it is simply used
                                    but not closed. if no session is provided,
                                    then one is open and closed at the end.
    """

    # BASIC SETTINGS & INITIALIZATION
    names = [names] if isinstance(names, str) else names
    inner_session = get_session() if session is None else session

    # QUERYING THE DATABASE
    results = (inner_session
               .query(schema.Persons.Id, schema.Persons.Full_Name)
               .filter(schema.Persons.Full_Name.in_(names))
               .all())

    # CLOSING AUXILIARY SESSIONS
    if session is None:
        inner_session.close()

    # FUNCTION OUTPUT
    return {full_name: code_id for code_id, full_name in results}


# QUERIES TESTING
# print(get_persons_id(['Alexandre De La Patellière',
#                       'Pierfrancesco Favino',
#                       'Adèle Simphal']))