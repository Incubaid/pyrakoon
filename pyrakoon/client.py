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

import logging
import functools

from pyrakoon import protocol, utils

LOGGER = logging.getLogger(__name__)
'''Logger for code in this module''' #pylint: disable-msg=W0105

def validate_types(specs, args):
    '''Validate method call argument types

    :param specs: Spec of expected types
    :type specs: iterable of (`str`, `pyrakoon.protocol.Type`,)
    :param args: Argument values
    :type args: iterable of `object`

    :raise TypeError: Type of an argument is invalid
    :raise ValueError: Value of an argument is invalid
    '''

    for spec, arg in zip(specs, args):
        name, type_ = spec[:2]

        try:
            type_.check(arg)
        except TypeError:
            raise TypeError('Invalid type of argument "%s"' % name)
        except ValueError:
            raise ValueError('Invalid value of argument "%s"' % name)


def call(message_type):
    '''Expose a `pyrakoon.protocol.Message` as a method on a client

    :param message_type: Type of the message this method should call
    :type message_type: `type`

    :return: Method which wraps a call to an Arakoon server using given message
        type
    :rtype: `callable`
    '''

    def wrapper(fun):
        '''Decorator helper'''

        # Calculate argspec of final method
        argspec = ['self']
        for arg in message_type.ARGS:
            if len(arg) == 2:
                argspec.append(arg[0])
            elif len(arg) == 3:
                argspec.append((arg[0], arg[2]))
            else:
                raise ValueError

        @utils.update_argspec(*argspec) #pylint: disable-msg=W0142
        @functools.wraps(fun)
        def wrapped(**kwargs): #pylint: disable-msg=C0111
            self = kwargs['self']

            if not self.connected:
                raise RuntimeError('Not connected')

            args = tuple(kwargs[arg[0]] for arg in message_type.ARGS)
            validate_types(message_type.ARGS, args)

            message = message_type(*args) #pylint: disable-msg=W0142

            return self._process(message) #pylint: disable-msg=W0212

        wrapped.__doc__ = message_type.DOC #pylint: disable-msg=W0622

        return wrapped

    return wrapper


#pylint: disable-msg=C0111,R0904
class Client(object):
    '''Abstract base class for implementations of Arakoon clients'''

    connected = False

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

    __getitem__ = get
    __setitem__ = set
    __delitem__ = delete
    __contains__ = exists

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

#pylint: enable-msg=C0111


class SocketClient(Client):
    '''Arakoon client using TCP to contact the cluster'''

    def __init__(self):
        import threading

        super(SocketClient, self).__init__()

        self._lock = threading.Lock()

        self._socket = None

    def connect(self):
        '''Create client socket and connect to server'''

        import socket

        self._socket = socket.create_connection(('127.0.0.1', 4000))

    @property
    def connected(self):
        '''Check whether a connection is available'''

        return self._socket is not None

    def _process(self, message):
        self._lock.acquire()

        try:
            for part in message.serialize():
                self._socket.sendall(part)

            return utils.read_blocking(message.receive(), self._socket.recv)
        except Exception:
            try:
                self._socket.close()
            finally:
                self._socket = None

            raise
        finally:
            self._lock.release()
