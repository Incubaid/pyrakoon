# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2010 Incubaid BVBA
#
# Licensees holding a valid Incubaid license may use this file in
# accordance with Incubaid's Arakoon commercial license agreement. For
# more information on how to enter into this agreement, please contact
# Incubaid (contact details can be found on www.arakoon.org/licensing).
#
# Alternatively, this file may be redistributed and/or modified under
# the terms of the GNU Affero General Public License version 3, as
# published by the Free Software Foundation. Under this license, this
# file is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# See the GNU Affero General Public License for more details.
# You should have received a copy of the
# GNU Affero General Public License along with this program (file "COPYING").
# If not, see <http://www.gnu.org/licenses/>.

'''Tests for code in `pyrakoon.tx`'''

import itertools

from twisted.internet import defer, error
from twisted.trial import unittest

from pyrakoon import errors, protocol, tx

bytes_ = lambda str_: (ord(c) for c in str_)

class _FakeTransport(object):
    disconnecting = False

    def __init__(self, owner, expected, to_send):
        self._owner = owner
        self._expected = expected
        self._to_send = to_send

        self.protocol = None

        self._received = []

        self.loseConnectionDeferred = defer.Deferred()

    def write(self, data):
        self._received.extend(data)

        if len(self._received) < len(self._expected):
            return

        self._owner.assertEquals(self._expected, ''.join(self._received))

        self.protocol.dataReceived(self._to_send)

    def loseConnection(self):
        self.disconnecting = True
        self.connected = False

        self.loseConnectionDeferred.callback(None)


class TestTwistedClient(unittest.TestCase):
    '''Tests for the Twisted client'''

    @staticmethod
    def _create_client(transport):
        protocol_ = tx.ArakoonProtocol()
        protocol_.makeConnection(transport)

        transport.protocol = protocol_

        return protocol_

    def test_who_master(self):
        '''Test a successful 'who_master' (no-argument) call'''

        expected = ''.join(protocol.WhoMaster().serialize())
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

        expected = ''.join(protocol.Hello('testsuite').serialize())
        to_send = ''.join(chr(i) for i in itertools.chain(
            (0, 0, 0, 0),
            (11, 0, 0, 0),
            bytes_('arakoon/1.0'),
        ))

        client = self._create_client(_FakeTransport(self, expected, to_send))

        deferred = client.hello('testsuite')
        deferred.addCallback(
            lambda value: self.assertEquals(value, 'arakoon/1.0'))

        return deferred

    def test_delete(self):
        '''Test a successful 'delete' (void) call'''

        expected = ''.join(protocol.Delete('key').serialize())
        to_send = ''.join(chr(i) for i in (0, 0, 0, 0))

        client = self._create_client(_FakeTransport(self, expected, to_send))

        deferred = client.delete('key')
        deferred.addCallback(lambda value: self.assertEquals(value, None))

        return deferred

    def test_not_found_exception(self):
        '''Test a failing 'get' call'''

        expected = ''.join(protocol.Get('key').serialize())
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

        expected = ''.join(protocol.Hello('testsuite').serialize())
        # This is not enough data, so we can test disconnect
        to_send = chr(0)

        client = self._create_client(_FakeTransport(self, expected, to_send))

        deferred = client.hello('testsuite')
        deferred.addErrback(
            lambda exc: exc.trap(error.ConnectionDone))

        client.connectionLost()

        return deferred

    def test_data_received_without_handler(self):
        '''Test behaviour when data is received when no handler is registered'''

        client = self._create_client(_FakeTransport(self, '', ''))

        client.dataReceived(''.join(chr(i) for i in (0, 0, 0, 0)))

        return client.transport.loseConnectionDeferred
