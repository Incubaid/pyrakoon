# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2010 Incubaid BVBA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''Tests for code in `pyrakoon.tx`'''

import itertools

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from twisted.internet import defer, error
from twisted.trial import unittest

from pyrakoon import client, errors, protocol, tx

bytes_ = lambda str_: (ord(c) for c in str_)

class _FakeTransport(object):
    disconnecting = False

    def __init__(self, owner, expected, to_send):
        self._owner = owner
        self._expected = expected
        self._to_send = to_send

        self.protocol = None

        self._received = StringIO.StringIO()

        self.loseConnectionDeferred = defer.Deferred()

    def write(self, data):
        self._received.write(data)

        if self._received.tell() < len(self._expected):
            return

        self._owner.assertEquals(self._expected, self._received.getvalue())

        if self.protocol:
            self.protocol.dataReceived(self._to_send)

    def writeSequence(self, data):
        map(self.write, data)

    def loseConnection(self):
        self.disconnecting = True
        self.connected = False

        self.loseConnectionDeferred.callback(None)


class ArakoonClientProtocol(tx.ArakoonProtocol, client.ClientMixin):
    '''Twisted Arakoon client protocol'''


class TestTwistedClient(unittest.TestCase):
    '''Tests for the Twisted client'''

    CLUSTER_ID = 'test_twisted_client'

    @classmethod
    def _create_client(cls, transport):
        protocol_ = ArakoonClientProtocol(cls.CLUSTER_ID)
        protocol_.makeConnection(transport)

        transport.protocol = protocol_

        return protocol_

    def test_who_master(self):
        '''Test a successful 'who_master' (no-argument) call'''

        expected = protocol.build_prologue(self.CLUSTER_ID)
        expected += ''.join(protocol.WhoMaster().serialize())
        to_send = ''.join(chr(i) for i in itertools.chain(
            (0, 0, 0, 0),
            (1,),
            (6, 0, 0, 0),
            bytes_('master'),
        ))

        client = self._create_client(_FakeTransport(self, expected, to_send))

        deferred = client.who_master()
        deferred.addCallback(lambda value: self.assertEquals(value, 'master'))

        return deferred

    def test_hello(self):
        '''Test a successful 'hello' call'''

        expected = protocol.build_prologue(self.CLUSTER_ID)
        expected += ''.join(protocol.Hello('testsuite',
            'pyrakoon_test').serialize())
        to_send = ''.join(chr(i) for i in itertools.chain(
            (0, 0, 0, 0),
            (11, 0, 0, 0),
            bytes_('arakoon/1.0'),
        ))

        client = self._create_client(_FakeTransport(self, expected, to_send))

        deferred = client.hello('testsuite', 'pyrakoon_test')
        deferred.addCallback(
            lambda value: self.assertEquals(value, 'arakoon/1.0'))

        return deferred

    def test_delete(self):
        '''Test a successful 'delete' (void) call'''

        expected = protocol.build_prologue(self.CLUSTER_ID)
        expected += ''.join(protocol.Delete('key').serialize())
        to_send = ''.join(chr(i) for i in (0, 0, 0, 0))

        client = self._create_client(_FakeTransport(self, expected, to_send))

        deferred = client.delete('key')
        deferred.addCallback(lambda value: self.assertEquals(value, None))

        return deferred

    def test_not_found_exception(self):
        '''Test a failing 'get' call'''

        expected = protocol.build_prologue(self.CLUSTER_ID)
        expected += ''.join(protocol.Get(False, 'key').serialize())
        to_send = ''.join(chr(i) for i in itertools.chain(
            (errors.NotFound.CODE, 0, 0, 0),
            (3, 0, 0, 0),
            bytes_('key'),
        ))

        client = self._create_client(_FakeTransport(self, expected, to_send))

        deferred = client.get('key')
        deferred.addErrback(lambda exc: exc.trap(errors.NotFound))

        return deferred

    def test_disconnect(self):
        '''Test disconnect'''

        expected = protocol.build_prologue(self.CLUSTER_ID)
        expected += ''.join(protocol.Hello('testsuite',
            'pyrakoon_test').serialize())
        # This is not enough data, so we can test disconnect
        to_send = chr(0)

        client = self._create_client(_FakeTransport(self, expected, to_send))

        deferred = client.hello('testsuite', 'pyrakoon_test')
        deferred.addErrback(
            lambda exc: exc.trap(error.ConnectionDone))

        client.connectionLost()

        return deferred

    def test_data_received_without_handler(self):
        '''Test behaviour when data is received when no handler is registered'''

        expected = protocol.build_prologue(self.CLUSTER_ID)
        client = self._create_client(_FakeTransport(self, expected, ''))

        client.dataReceived(''.join(chr(i) for i in (0, 0, 0, 0)))

        return client.transport.loseConnectionDeferred
