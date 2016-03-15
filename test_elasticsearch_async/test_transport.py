import asyncio

from pytest import mark

from elasticsearch_async import AsyncElasticsearch

@mark.asyncio
def test_sniff_on_start_sniffs(server, event_loop, port, sniff_data):
    futures = []
    def task_factory(loop, coro):
        t = asyncio.Task(coro, loop=loop)
        futures.append(t)
        return t
    event_loop.set_task_factory(task_factory)

    server.register_response('/_nodes/_all/clear', sniff_data)

    client = AsyncElasticsearch(port=port, sniff_on_start=True, loop=event_loop)

    # sniff has been called in the background
    assert len(futures) == 1
    yield from futures[0]

    assert [('GET', '/_nodes/_all/clear', '', {})] == server.calls
    connections = client.transport.connection_pool.connections

    assert 1 == len(connections)
    assert 'http://node1:9200' == connections[0].host

@mark.asyncio
def test_retry_will_work(port, server, event_loop):
    client = AsyncElasticsearch(hosts=['not-an-es-host', 'localhost'], port=port, loop=event_loop, randomize_hosts=False)

    data = yield from client.info()
    assert  {'body': '', 'method': 'GET', 'params': {}, 'path': '/'} == data
    client.transport.close()
