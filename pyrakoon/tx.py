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

'''Twisted_ protocol implementation for Arakoon_

.. _Twisted: http://www.twistedmatrix.com
.. _Arakoon: http://www.arakoon.org
'''

import collections

from twisted.internet import defer, protocol as twisted_protocol
from twisted.protocols import basic, stateful
from twisted.python import log

from pyrakoon import client, errors, protocol, utils

#pylint: disable-msg=R0904,C0103,R0901

# R0904: Too many public methods
# C0103: Invalid name
# R0901: Too many ancestors

try:
    _PauseableMixin = basic._PauseableMixin #pylint: disable-msg=C0103,W0212
except AttributeError:
    class _PauseableMixin: #pylint: disable-msg=W0232
        '''Mixin to add *IProducer* support to a protocol'''

        paused = False

        def pauseProducing(self):
            '''Pause producing'''

            self.paused = True
            self.transport.pauseProducing()

        def resumeProducing(self):
            '''Resume producing'''

            self.paused = False
            self.transport.resumeProducing()
            self.dataReceived('')

        def stopProducing(self):
            '''Stop producing'''

            self.paused = True
            self.transport.stopProducing()


class ArakoonProtocol(object,
    client.AbstractClient,
    stateful.StatefulProtocol, _PauseableMixin):
    '''Protocol to access an Arakoon server'''

    _INITIAL_REQUEST_SIZE = protocol.UINT32.PACKER.size
    connected = False

    def __init__(self, cluster_id):
        '''Initialize a new `ArakoonProtocol`

        :param cluster_id: Name of the cluster
        :type cluster_id: `str`
        '''

        super(ArakoonProtocol, self).__init__()

        self._outstanding = collections.deque()
        self._currentHandler = None

        self._cluster_id = cluster_id

    def _process(self, message):
        if not self.connected:
            return defer.fail(
                client.NotConnectedError('Protocol not connected'))

        deferred = defer.Deferred()
        self._outstanding.append((message.receive, deferred))

        data = list(message.serialize())
        self.transport.writeSequence(data)

        return deferred

    def getInitialState(self):
        assert self._currentHandler == None

        return self._responseCodeReceived, self._INITIAL_REQUEST_SIZE

    def _responseCodeReceived(self, data):
        '''Handler for server command response codes'''

        assert self._currentHandler == None

        try:
            handler = self._outstanding.popleft()
        except IndexError:
            log.msg('Request data received but no handler registered')
            self.transport.loseConnection()

            return None

        receiver = handler[0]()
        self._currentHandler = (receiver, handler[1])

        request = receiver.next()

        if isinstance(request, protocol.Result):
            return self._handleResult(request)
        elif isinstance(request, protocol.Request):
            if request.count != self._INITIAL_REQUEST_SIZE:
                handler[1].errback(ValueError('Unexpected request count'))
                self.transport.loseConnection()

                return None

            return self._handleRequest(data)
        else:
            log.err(TypeError,
                'Received unknown type from message parsing coroutine')
            handler[1].errback(TypeError)

            self.transport.loseConnection()

            return None


    def _handleRequest(self, data):
        '''Handler for `Request` values emitted by a message decoder'''

        if not self._currentHandler:
            log.msg('Request data received but no handler registered')
            self.transport.loseConnection()

            return None

        receiver, deferred = self._currentHandler

        try:
            request = receiver.send(data)
        except Exception, exc: #pylint: disable-msg=W0703
            if not isinstance(exc, errors.ArakoonError):
                log.err(exc, 'Exception raised by message receive loop')

                deferred.errback(exc)
                self.transport.loseConnection()

                return None
            else:
                deferred.errback(exc)

                self._currentHandler = None
                utils.kill_coroutine(receiver, lambda msg: log.err(None, msg))

                return self.getInitialState()

        if isinstance(request, protocol.Result):
            return self._handleResult(request)
        elif isinstance(request, protocol.Request):
            return self._handleRequest, request.count
        else:
            log.err(TypeError,
                'Received unknown type from message parsing coroutine')
            deferred.errback(TypeError)

            self.transport.loseConnection()

            return None

    def _handleResult(self, result):
        '''Handler for `Result` values emitted by a message decoder'''

        receiver, deferred = self._currentHandler
        self._currentHandler = None

        deferred.callback(result.value)

        # To be on the safe side...
        utils.kill_coroutine(receiver, lambda msg: log.err(None, msg))

        return self.getInitialState()

    def connectionMade(self):
        prologue = protocol.build_prologue(self._cluster_id)
        self.transport.write(prologue)

        self.connected = True

        return stateful.StatefulProtocol.connectionMade(self)

    def connectionLost(self, reason=twisted_protocol.connectionDone):
        self.connected = False

        self._cancelHandlers(reason)

        return stateful.StatefulProtocol.connectionLost(self, reason)

    def _cancelHandlers(self, reason):
        '''Cancel all pending handlers

        This will call the errback of all pending handlers using `reason`.

        :param reason: Reason for cancelation
        :type reason: `twisted.python.failure.Failure`
        '''

        if self._currentHandler:
            log.msg('Canceling current handler')

            receiver, deferred = self._currentHandler
            self._currentHandler = None

            if not deferred.called:
                deferred.errback(reason)

            utils.kill_coroutine(receiver, lambda msg: log.err(None, msg))

        log.msg('Canceling %d outstanding requests' % len(self._outstanding))

        while True:
            try:
                _, deferred = self._outstanding.popleft()
            except IndexError:
                break

            deferred.errback(reason)

        assert len(self._outstanding) == 0
