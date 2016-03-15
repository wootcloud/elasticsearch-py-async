import asyncio
import json
from urllib.parse import urlparse, parse_qsl

from aiohttp.server import ServerHttpProtocol
from aiohttp import Response

from pytest import yield_fixture, fixture

from elasticsearch_async import AIOHttpConnection, AsyncElasticsearch


@yield_fixture
def connection(event_loop, server, port):
    connection = AIOHttpConnection(port=port, loop=event_loop)
    yield connection
    event_loop.run_until_complete(connection.close())

class DummyElasticsearch(ServerHttpProtocol):
    @asyncio.coroutine
    def handle_request(self, message, payload):
        url = urlparse(message.path)

        params = dict(parse_qsl(url.query))
        body = yield from payload.read()
        body = body.decode('utf-8')
        if body:
            body = json.loads(body)
            print(body)
        out = {
            'method': message.method,
            'params': params,
            'path': url.path,
            'body': body
        }
        out = json.dumps(out).encode('utf-8')

        response = Response(
            self.writer, params.get('status', 200), http_version=message.version
        )
        response.add_header('Content-Type', 'application/json')
        response.add_header('Content-Length', str(len(out)))
        response.send_headers()
        response.write(out)
        yield from response.write_eof()

i = 0
@fixture
def port():
    global i
    i += 1
    return 8080 + i

@fixture
def server(event_loop, port):
    f = event_loop.create_server(
        lambda: DummyElasticsearch(debug=True, keep_alive=75),
        '127.0.0.1', port
    )
    return event_loop.run_until_complete(f)

@yield_fixture
def client(event_loop, server, port):
    c = AsyncElasticsearch([{'host': '127.0.0.1','port': port}], loop=event_loop)
    yield c
    c.transport.close()
