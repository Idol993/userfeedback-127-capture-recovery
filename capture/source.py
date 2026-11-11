"""HTTP client for the local supplier simulator.

Failures are split into two explicit families so the worker never advances a
checkpoint for a response that was never safely consumed:

* :class:`RetryableSourceError` - transport hiccups, 429 with Retry-After,
  transient 5xx, malformed JSON. Safe to retry, the request is idempotent.
* :class:`ProtocolError` - the server answered but the exchange violates the
  contract (unknown snapshot, malformed payload schema, non-retryable status).
  Retrying verbatim cannot help; the job must stop and keep its checkpoint.
"""
import json

import httpx


class SourceError(RuntimeError):
    """Base class for every failure raised by :class:`Source`."""


class RetryableSourceError(SourceError):
    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


class ProtocolError(SourceError):
    pass


_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_REQUIRED_ORDER_FIELDS = ('order_id', 'version')


class Source:
    def __init__(self, url, timeout=10.0):
        self.client = httpx.Client(
            base_url=url.rstrip('/'), timeout=timeout, trust_env=False,
        )

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _send(self, method, path, params=None):
        try:
            response = self.client.request(method, path, params=params)
        except httpx.TransportError as exc:
            raise RetryableSourceError('transport error: %s' % exc) from exc
        if response.status_code in _RETRYABLE_STATUS:
            retry_after = None
            if 'Retry-After' in response.headers:
                try:
                    retry_after = float(response.headers['Retry-After'])
                except ValueError:
                    retry_after = None
            raise RetryableSourceError(
                'upstream status %s' % response.status_code, retry_after,
            )
        if response.status_code >= 400:
            # 410 (snapshot expired), 404 and any other non-retryable status
            # are contract violations: retrying would not change the answer.
            raise ProtocolError(
                'non-retryable upstream status %s: %s'
                % (response.status_code, response.text[:200]),
            )
        try:
            payload = json.loads(response.text)
        except ValueError as exc:
            raise RetryableSourceError('malformed JSON body: %s' % exc) from exc
        if not isinstance(payload, dict):
            raise ProtocolError('response body is not a JSON object')
        return payload

    @staticmethod
    def _validate_items(payload, endpoint):
        items = payload.get('items')
        if not isinstance(items, list):
            raise ProtocolError('%s response missing items list' % endpoint)
        for item in items:
            if not isinstance(item, dict):
                raise ProtocolError('%s contains a non-object item' % endpoint)
            for field in _REQUIRED_ORDER_FIELDS:
                if field not in item:
                    raise ProtocolError(
                        '%s item missing required field %r' % (endpoint, field),
                    )
            if not isinstance(item['version'], int):
                raise ProtocolError('%s item has non-integer version' % endpoint)
        return items

    def snapshot(self):
        payload = self._send('POST', '/snapshots')
        if 'snapshot_id' not in payload or 'change_seq' not in payload:
            raise ProtocolError('snapshot response missing snapshot_id/change_seq')
        return payload

    def page(self, snapshot_id, cursor):
        payload = self._send(
            'GET', '/snapshots/' + snapshot_id, {'cursor': cursor or '0'},
        )
        if payload.get('snapshot_id') != snapshot_id:
            raise ProtocolError('page snapshot_id does not match the request')
        if 'next_cursor' not in payload:
            raise ProtocolError('page response missing next_cursor')
        self._validate_items(payload, 'page')
        return payload

    def changes(self, seq):
        payload = self._send('GET', '/changes', {'after': seq})
        if 'next_seq' not in payload:
            raise ProtocolError('changes response missing next_seq')
        self._validate_items(payload, 'changes')
        return payload
