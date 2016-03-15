import asyncio
import json
import logging

import aiohttp

from pytest import mark, yield_fixture, raises

from elasticsearch import NotFoundError

from elasticsearch_async.connection import AIOHttpConnection

@mark.asyncio
def test_info(connection):
    status, headers, data = yield from connection.perform_request('GET', '/')

    data = json.loads(data)

    assert status == 200
    assert  {'body': '', 'method': 'GET', 'params': {}, 'path': '/'} == data

def test_auth_is_set_correctly():
    connection = AIOHttpConnection(http_auth=('user', 'secret'))
    assert connection.session._default_auth == aiohttp.BasicAuth('user', 'secret')

    connection = AIOHttpConnection(http_auth='user:secret')
    assert connection.session._default_auth == aiohttp.BasicAuth('user', 'secret')

@mark.asyncio
def test_request_is_properly_logged(connection, caplog, port):
    yield from connection.perform_request('GET', '/_cat/indices', body=b'{}', params={"format": "json"})

    for logger, level, message in caplog.record_tuples:
        if logger == 'elasticsearch' and level == logging.INFO:
            assert message.startswith('GET http://localhost:%s/_cat/indices?format=json [status:200 request:' % port)
            break
    else:
        assert False, 'Message not found'

    assert ('elasticsearch', logging.DEBUG, '> {}') in caplog.record_tuples

@mark.asyncio
def test_error_is_properly_logged(connection, caplog, port):
    with raises(NotFoundError):
        yield from connection.perform_request('GET', '/not-here', params={'status': 404})

    for logger, level, message in caplog.record_tuples:
        if logger == 'elasticsearch' and level == logging.WARNING:
            assert message.startswith('GET http://localhost:%s/not-here?status=404 [status:404 request:' % port)
            break
    else:
        assert False, "Log not received"
