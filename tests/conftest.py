from os import environ

import pytest
import pytest_asyncio

import pgwasm.dbapi


@pytest.fixture(scope="class")
def db_kwargs():
    db_connect = {
        "user": "postgres",
        "password": "pw",
        "uri": f"ws://{environ['PGHOST']}:{environ['PGPORT']}"
    }
    return db_connect


@pytest_asyncio.fixture
async def con(request, db_kwargs, event_loop):
    conn = await pgwasm.dbapi.connect(**db_kwargs)

    def fin():
        async def afin():
            try:
                await conn.rollback()
            except pgwasm.dbapi.InterfaceError:
                pass

            try:
                await conn.close()
            except pgwasm.dbapi.InterfaceError:
                pass
        event_loop.run_until_complete(afin())

    request.addfinalizer(fin)
    return conn


@pytest_asyncio.fixture
async def cursor(request, con):
    cursor = con.cursor()

    def fin():
        cursor.close()

    request.addfinalizer(fin)
    return cursor


@pytest_asyncio.fixture
async def pg_version(cursor):
    await cursor.execute("select current_setting('server_version')")
    retval = cursor.fetchall()
    version = retval[0][0]
    idx = version.index(".")
    return int(version[:idx])
