from pytest import mark, raises

from elasticsearch import NotFoundError

@mark.asyncio
def test_info_works(client):
    data = yield from client.info()

    assert data['tagline'] == 'You Know, for Search'

@mark.asyncio
def test_ping_works(client):
    data = yield from client.ping()

    assert data is True

@mark.asyncio
def test_exists_with_404_returns_false(client):
    data = yield from client.indices.exists(index='not-there')

    assert data is False

@mark.asyncio
def test_404_properly_raised(client):
    with raises(NotFoundError):
        yield from client.get(index='not-there', doc_type='t', id=42)

@mark.asyncio
def test_params_and_body_get_passed_properly(client):
    response = {
        'tokens': [
            {
                'end_offset': 12,
                'position': 0,
                'start_offset': 0,
                'token': 'Hello World!',
                'type': 'word'
            }
        ]
    }

    data = yield from client.indices.analyze(body={'analyzer': 'keyword', 'text': 'Hello World!'})
    assert data == response

    data = yield from client.indices.analyze(**{'analyzer': 'keyword', 'text': 'Hello World!'})
    assert data == response
