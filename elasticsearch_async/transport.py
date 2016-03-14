import asyncio

from elasticsearch import Transport, TransportError, ConnectionTimeout, ConnectionError

from .connection import AIOHttpConnection

class AsyncTransport(Transport):
    def __init__(self, hosts, connection_class=AIOHttpConnection, **kwargs):
        # TODO: if sniff_on_start, pass False to super and call our coroutine directly
        super().__init__(hosts, connection_class=connection_class, **kwargs)

    @asyncio.coroutine
    def main_loop(self, method, url, params, body, ignore=(), timeout=None):
        for attempt in range(self.max_retries + 1):
            connection = self.get_connection()

            try:
                status, headers, data = yield from connection.perform_request(
                    method, url, params, body, ignore=ignore, timeout=timeout)
            except TransportError as e:
                if method == 'HEAD' and e.status_code == 404:
                    return False

                retry = False
                if isinstance(e, ConnectionTimeout):
                    retry = self.retry_on_timeout
                elif isinstance(e, ConnectionError):
                    retry = True
                elif e.status_code in self.retry_on_status:
                    retry = True

                if retry:
                    # only mark as dead if we are retrying
                    self.mark_dead(connection)
                    # raise exception on last retry
                    if attempt == self.max_retries:
                        raise
                else:
                    raise

            else:
                if method == 'HEAD':
                    return 200 <= status < 300

                # connection didn't fail, confirm it's live status
                self.connection_pool.mark_live(connection)
                if data:
                    data = self.deserializer.loads(data, headers.get('content-type'))
                return data

    def perform_request(self, method, url, params=None, body=None):
        if body is not None:
            body = self.serializer.dumps(body)

            # some clients or environments don't support sending GET with body
            if method in ('HEAD', 'GET') and self.send_get_body_as != 'GET':
                # send it as post instead
                if self.send_get_body_as == 'POST':
                    method = 'POST'

                # or as source parameter
                elif self.send_get_body_as == 'source':
                    if params is None:
                        params = {}
                    params['source'] = body
                    body = None

        if body is not None:
            try:
                body = body.encode('utf-8')
            except (UnicodeDecodeError, AttributeError):
                # bytes/str - no need to re-encode
                pass

        ignore = ()
        timeout = None
        if params:
            timeout = params.pop('request_timeout', None)
            ignore = params.pop('ignore', ())
            if isinstance(ignore, int):
                ignore = (ignore, )

        return asyncio.ensure_future(self.main_loop(method, url, params, body,
                                                    ignore=ignore,
                                                    timeout=timeout))
