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
    assert data['tagline'] == "You Know, for Search"

def test_auth_is_set_correctly():
    connection = AIOHttpConnection(http_auth=('user', 'secret'))
    assert connection.session._default_auth == aiohttp.BasicAuth('user', 'secret')

    connection = AIOHttpConnection(http_auth='user:secret')
    assert connection.session._default_auth == aiohttp.BasicAuth('user', 'secret')

@mark.asyncio
def test_request_is_properly_logged(connection, caplog):
    yield from connection.perform_request('GET', '/_cat/indices', body=b'{}', params={"format": "json"})

    for logger, level, message in caplog.record_tuples:
        if logger == 'elasticsearch' and level == logging.INFO:
            assert message.startswith('GET http://localhost:9200/_cat/indices?format=json [status:200 request:')
            break
    else:
        assert False, 'Message not found'

    assert ('elasticsearch', logging.DEBUG, '> {}') in caplog.record_tuples
    assert ('elasticsearch', logging.DEBUG, '< []') in caplog.record_tuples

@mark.asyncio
def test_error_is_properly_logged(connection, caplog):
    with raises(NotFoundError):
        yield from connection.perform_request('GET', '/not-here', params={"some": "data"})

    for logger, level, message in caplog.record_tuples:
        if logger == 'elasticsearch' and level == logging.WARNING:
            assert message.startswith('GET http://localhost:9200/not-here?some=data [status:404 request:')
            break
    else:
        assert False, "Log not received"
