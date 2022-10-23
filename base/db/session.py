"""
SQLAlchemy session.
"""

from sqlalchemy import orm


from .engine import get_engine


Session = orm.sessionmaker()


def ScopedSession():
    """
    Yields a scoped database session.

    :param engine: The database engine.
    :type engine: sqlalchemy.engine
    :yield: The database session.
    :rtype: sqlalchemy.orm.session
    """
    engine = get_engine()
    Session.configure(autocommit=False, autoflush=False, bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
        Session.configure(bind=None)
