from typing import Callable, Optional

import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

__factory: Optional[Callable[[], Session]] = None
__engine: Optional[Engine] = None


def create_session(conn_string: str = None) -> Session:
    global __factory

    if conn_string is None:
        conn_string = _create_connection_string()

    if not __factory:
        engine = create_engine(conn_string)
        __factory = orm.sessionmaker(bind=engine)

    session: Session = __factory()
    session.expire_on_commit = False

    return session

# TODO: make this read env variables
def _create_connection_string()-> str:
    """This is a shortcut"""
    return 'mysql+pymysql://root:password@host.docker.internal:3306/real_estate_db'
    # return 'mysql+pymysql://root:password@localhost:3306/real_estate_db'
    # return 'mysql+pymysql://root:password@localhost:3306/dw_real_estate'


def create_engine(conn_string: str) -> Engine:
    global __engine

    if __engine is None:
        __engine = sa.create_engine(conn_string)

    return __engine

