from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nyp.config import MYSQL_CON

engine = create_engine(MYSQL_CON)

Session = sessionmaker(engine)


@contextmanager
def wrapped_session():
    """Provide a transactional scope around a series of operations."""
    s = Session()
    try:
        yield s
        s.commit()
    except Exception as e:
        s.rollback()
        raise e
    finally:
        s.close()
