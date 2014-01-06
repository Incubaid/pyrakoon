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

'''Arakoon client interface'''

from pyrakoon import errors, protocol
import pyrakoon.utils
from pyrakoon.client.utils import call

class ClientMixin: #pylint: disable-msg=W0232,R0904
    '''Mixin providing client actions for standard cluster functionality

    This can be mixed into any class implementing `AbstractClient`.

    :see: `AbstractClient`
    '''

    #pylint: disable-msg=C0111
    @call(protocol.Hello)
    def hello(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Exists)
    def exists(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.WhoMaster)
    def who_master(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Get)
    def get(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Set)
    def set(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Delete)
    def delete(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.PrefixKeys)
    def prefix(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.TestAndSet)
    def test_and_set(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Sequence)
    def sequence(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Range)
    def range(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.RangeEntries)
    def range_entries(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.MultiGet)
    def multi_get(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.ExpectProgressPossible)
    def expect_progress_possible(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.GetKeyCount)
    def get_key_count(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.UserFunction)
    def user_function(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Confirm)
    def confirm(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Assert)
    def assert_(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.RevRangeEntries)
    def rev_range_entries(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Statistics)
    def statistics(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Version)
    def version(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.AssertExists)
    def assert_exists(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.DeletePrefix)
    def delete_prefix(self): #pylint: disable-msg=R0201
        assert False

    @call(protocol.Nop)
    def nop(self): #pylint: disable-msg=R0201
        assert False

    __getitem__ = get
    __setitem__ = set
    __delitem__ = delete
    __contains__ = exists


class NotConnectedError(RuntimeError):
    '''Error used when a call on a not-connected client is made'''


class AbstractClient: #pylint: disable-msg=W0232,R0903,R0922
    '''Abstract base class for implementations of Arakoon clients'''

    connected = False

    def _process(self, message):
        '''
        Submit a message to the server, parse the result and return it

        The given `message` should be serialized using its `serialize` method
        and submitted to the server. Then the `receive` coroutine of the message
        should be used to retrieve and parse a result from the server. The
        returned value should be returned by this method, or any exceptions
        should be rethrown if caught.

        :param message: Message to handle
        :type message: `pyrakoon.protocol.Message`

        :return: Server result value
        :rtype: `object`

        :see: `pyrakoon.protocol.Message.serialize`
        :see: `pyrakoon.protocol.Message.receive`
        :see: `pyrakoon.utils.process_blocking`
        '''

        raise NotImplementedError


#pylint: disable-msg=R0904
class SocketClient(object, AbstractClient):
    '''Arakoon client using TCP to contact the cluster'''

    def __init__(self, address, cluster_id):
        import threading

        super(SocketClient, self).__init__()

        self._lock = threading.Lock()

        self._socket = None
        self._address = address
        self._cluster_id = cluster_id

    def connect(self):
        '''Create client socket and connect to server'''

        import socket

        self._socket = socket.create_connection(self._address)
        prologue = protocol.build_prologue(self._cluster_id)
        self._socket.sendall(prologue)

    @property
    def connected(self):
        '''Check whether a connection is available'''

        return self._socket is not None

    def _process(self, message):
        self._lock.acquire()

        try:
            for part in message.serialize():
                self._socket.sendall(part)

            return pyrakoon.utils.read_blocking(
                message.receive(), self._socket.recv)
        except Exception as exc:
            if not isinstance(exc, errors.ArakoonError):
                try:
                    if self._socket:
                        self._socket.close()
                finally:
                    self._socket = None

            raise
        finally:
            self._lock.release()
