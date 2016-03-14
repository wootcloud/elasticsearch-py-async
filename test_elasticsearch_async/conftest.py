import asyncio

from pytest import yield_fixture

from elasticsearch_async.connection import AIOHttpConnection

@yield_fixture
def connection(event_loop):
    connection = AIOHttpConnection()
    yield connection
    event_loop.run_until_complete(connection.close())
