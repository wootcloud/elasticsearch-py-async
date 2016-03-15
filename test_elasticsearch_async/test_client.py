from pytest import mark, raises

from elasticsearch import NotFoundError

@mark.asyncio
def test_info_works(client):
    data = yield from client.info()

    assert  {'body': '', 'method': 'GET', 'params': {}, 'path': '/'} == data

@mark.asyncio
def test_ping_works(client):
    data = yield from client.ping()

    assert data is True

@mark.asyncio
def test_exists_with_404_returns_false(client):
    data = yield from client.indices.exists(index='not-there', params={'status': 404})

    assert data is False

@mark.asyncio
def test_404_properly_raised(client):
    with raises(NotFoundError):
        yield from client.get(index='not-there', doc_type='t', id=42, params={'status': 404})

@mark.asyncio
def test_body_gets_passed_properly(client):
    data = yield from client.index(index='i', doc_type='t', id='42', body={'some': 'data'})
    assert  {'body': {'some': 'data'}, 'method': 'PUT', 'params': {}, 'path': '/i/t/42'} == data

@mark.asyncio
def test_params_get_passed_properly(client):
    data = yield from client.info(params={'some': 'data'})
    assert  {'body': '', 'method': 'GET', 'params': {'some': 'data'}, 'path': '/'} == data
