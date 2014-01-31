Twisted Support
===============
`pyrakoon` comes with an :class:`~pyrakoon.client.AbstractClient` implementation
supporting the `Twisted`_ framework, provided as a
:class:`~twisted.internet.protocol.Protocol` in the :mod:`pyrakoon.tx` module.

.. _Twisted: http://www.twistedmatrix.com

Here's a demonstration of how it could be used:

.. doctest::

    >>> from twisted.internet import defer, endpoints, reactor
    >>> from pyrakoon import client, tx

    >>> class Protocol(tx.ArakoonProtocol, client.ClientMixin): pass

    >>> @defer.inlineCallbacks
    ... def connected(proto):
    ...     yield proto.set('key', 'value')
    ...     value = yield proto.get('key')
    ...     print 'Value:', value
    ...     yield proto.delete('key')
    ...     reactor.stop()

    >>> endpoint = endpoints.TCP4ClientEndpoint(reactor, 'localhost', 4000)
    >>> d = endpoints.connectProtocol(endpoint, Protocol('ricky'))
    >>> d.addCallback(connected)
    <Deferred at ...>
    >>> reactor.run()
    Value: value
