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

'''Testing utilities'''

import struct
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from pyrakoon import client, errors, protocol

class FakeClient(client.Client):
    '''Fake, in-memory Arakoon client'''

    VERSION = 'FakeRakoon/0.1'
    '''Version of the server we fake''' #pylint: disable-msg=W0105
    MASTER = 'arakoon0'
    '''Name of master node''' #pylint: disable-msg=W0105

    connected = True

    def __init__(self):
        super(FakeClient, self).__init__()

        self._values = {}

    def _process(self, bytes_, receiver): #pylint: disable-msg=R0912
        bytes_ = StringIO.StringIO(''.join(bytes_)).read

        # Helper
        recv = lambda type_: client.process_blocking(type_.receive(), bytes_)

        command = recv(protocol.UNSIGNED_INTEGER)

        def handle_hello():
            '''Handle a "hello" command'''

            _ = recv(protocol.STRING)

            for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes
            for rbytes in protocol.STRING.serialize(self.VERSION):
                yield rbytes

        def handle_exists():
            '''Handle an "exists" command'''

            key = recv(protocol.STRING)

            for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes
            for rbytes in protocol.BOOL.serialize(key in self._values):
                yield rbytes

        def handle_who_master():
            '''Handle a "who_master" command'''

            for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes
            for rbytes in protocol.Option(protocol.STRING).serialize(
                self.MASTER):
                yield rbytes

        def handle_get():
            '''Handle a "get" command'''

            key = recv(protocol.STRING)

            if key not in self._values:
                for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                    errors.NotFound.CODE):
                    yield rbytes
                for rbytes in protocol.STRING.serialize(key):
                    yield rbytes
            else:
                for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                    protocol.RESULT_SUCCESS):
                    yield rbytes
                for rbytes in protocol.STRING.serialize(self._values[key]):
                    yield rbytes

        def handle_set():
            '''Handle a "set" command'''

            key = recv(protocol.STRING)
            value = recv(protocol.STRING)

            self._values[key] = value

            for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes

        def handle_delete():
            '''Handle a "delete" command'''

            key = recv(protocol.STRING)

            if key not in self._values:
                for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                    errors.NotFound.CODE):
                    yield rbytes
                for rbytes in protocol.STRING.serialize(key):
                    yield rbytes
            else:
                del self._values[key]
                for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                    protocol.RESULT_SUCCESS):
                    yield rbytes

        def handle_prefix_keys():
            '''Handle a "prefix_keys" command'''

            prefix = recv(protocol.STRING)
            max_elements = recv(protocol.UNSIGNED_INTEGER)

            matches = [key for key in self._values.iterkeys()
                if key.startswith(prefix)]

            matches = matches if max_elements < 0 else matches[:max_elements]

            for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes

            for rbytes in protocol.List(protocol.STRING).serialize(matches):
                yield rbytes

        def handle_test_and_set():
            '''Handle a "test_and_set" command'''

            key = recv(protocol.STRING)
            test_value = recv(protocol.Option(protocol.STRING))
            set_value = recv(protocol.Option(protocol.STRING))

            # Key doesn't exist and test_value is not None -> NotFound
            if key not in self._values and test_value is not None:
                for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                    errors.NotFound.CODE):
                    yield rbytes
                for rbytes in protocol.STRING.serialize(key):
                    yield rbytes

                return

            # Key doesn't exist and test_value is None -> create
            if key not in self._values and test_value is None:
                self._values[key] = set_value

                for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                    protocol.RESULT_SUCCESS):
                    yield rbytes
                for rbytes in protocol.Option(protocol.STRING).serialize(None):
                    yield rbytes

                return

            # Key exists
            orig_value = self._values[key]

            # Need to update?
            if test_value == orig_value:
                if set_value is not None:
                    self._values[key] = set_value
                else:
                    del self._values[key]

            # Return original value
            for rbytes in protocol.UNSIGNED_INTEGER.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes

            for rbytes in protocol.Option(protocol.STRING).serialize(
                orig_value):
                yield rbytes


        handlers = {
            protocol.Hello.TAG: handle_hello,
            protocol.Exists.TAG: handle_exists,
            protocol.WhoMaster.TAG: handle_who_master,
            protocol.Get.TAG: handle_get,
            protocol.Set.TAG: handle_set,
            protocol.Delete.TAG: handle_delete,
            protocol.PrefixKeys.TAG: handle_prefix_keys,
            protocol.TestAndSet.TAG: handle_test_and_set,
        }

        if command in handlers:
            result = StringIO.StringIO(''.join(handlers[command]()))
        else:
            result = StringIO.StringIO()
            result.write(struct.pack('<I', errors.UnknownFailure.CODE))
            result.write(struct.pack('<I', 0))
            result.seek(0)

        return client.process_blocking(receiver, result.read)
