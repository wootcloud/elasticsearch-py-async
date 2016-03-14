import asyncio

from pytest import yield_fixture

from elasticsearch_async import AIOHttpConnection, AsyncElasticsearch

@yield_fixture
def connection(event_loop):
    connection = AIOHttpConnection()
    yield connection
    event_loop.run_until_complete(connection.close())

@yield_fixture
def client(event_loop):
    c = AsyncElasticsearch()
    yield c
    event_loop.run_until_complete(c.transport.get_connection().close())
