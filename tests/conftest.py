import asyncio
from os import environ

import pytest
import pytest_asyncio

import pgwasm.dbapi


@pytest.fixture(scope="class")
def db_kwargs():
    db_connect = {
        "user": "postgres",
        "password": "pw",
        "uri": f"ws://{environ.get('PGHOST', 'localhost')}:{environ.get('PGPORT', 6432)}"
    }
    return db_connect


@pytest.fixture
def con(request, db_kwargs):
    conn = pgwasm.dbapi.connect(**db_kwargs)

    def fin():
        try:
            conn.rollback()
        except pgwasm.dbapi.InterfaceError:
            pass

        try:
            conn.close()
        except pgwasm.dbapi.InterfaceError:
            pass

    request.addfinalizer(fin)
    return conn


@pytest.fixture
def cursor(request, con):
    cursor = con.cursor()

    def fin():
        cursor.close()

    request.addfinalizer(fin)
    return cursor


@pytest.fixture
def pg_version(cursor):
    cursor.execute("select current_setting('server_version')")
    retval = cursor.fetchall()
    version = retval[0][0]
    idx = version.index(".")
    return int(version[:idx])
