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

'''Tests for code in `pyrakoon.compat`'''

import os.path
import time
import shutil
import logging
import tempfile
import unittest
import subprocess

import nose

from pyrakoon import compat

LOGGER = logging.getLogger(__name__)

DEFAULT_CLIENT_PORT = 4932
DEFAULT_MESSAGING_PORT = 4933

CONFIG_TEMPLATE = '''
[global]
cluster = arakoon_0
cluster_id = pyrakoon_test

[arakoon_0]
ip = 127.0.0.1
client_port = %(CLIENT_PORT)d
messaging_port = %(MESSAGING_PORT)d
home = %(HOME)s
log_dir = %(LOG_DIR)s
log_level = debug
'''

class TestArgumentChecks(unittest.TestCase):
    '''Test argument checking of `ArakoonClient` methods'''

    def test_hello(self):
        '''Test validation of the argument of the `hello` method'''

        client = compat.ArakoonClient(None)

        self.assertRaises(compat.ArakoonInvalidArguments, client.hello, 123)


class TestCompatClient(unittest.TestCase):
    '''Test the compatibility client against a real Arakoon server'''

    def setUp(self):
        self.base = tempfile.mkdtemp(prefix='pyrakoon_test')
        LOGGER.info('Running in %s', self.base)

        home_dir = os.path.join(self.base, 'home')
        os.mkdir(home_dir)

        log_dir = os.path.join(self.base, 'log')
        os.mkdir(log_dir)

        config_path = os.path.join(self.base, 'config.ini')
        config = CONFIG_TEMPLATE % {
            'CLIENT_PORT': DEFAULT_CLIENT_PORT,
            'MESSAGING_PORT': DEFAULT_MESSAGING_PORT,
            'HOME': home_dir,
            'LOG_DIR': log_dir,
        }

        fd = open(config_path, 'w')
        try:
            fd.write(config)
        finally:
            fd.close()

        # Start server
        command = ['arakoon', '-config', config_path, '--node', 'arakoon_0']
        self.process = subprocess.Popen(command, close_fds=True, cwd=self.base)

        LOGGER.info('Arakoon running, PID %d', self.process.pid)

        self.client_config = compat.ArakoonClientConfig('pyrakoon_test', {
            'arakoon_0': ('127.0.0.1', DEFAULT_CLIENT_PORT),
        })

        # Give server some time to get up
        ok = False
        for _ in xrange(5):
            LOGGER.info('Attempting hello call')
            try:
                client = self._create_client()
                client.hello('testsuite', 'pyrakoon_test')
                client._client.drop_connections()
            except:
                LOGGER.info('Call failed, sleeping')
                time.sleep(1)
            else:
                LOGGER.debug('Call succeeded')
                ok = True
                break

        if not ok:
            raise RuntimeError('Unable to start Arakoon server')

    def tearDown(self):
        try:
            if self.process:
                pid = self.process.pid

                LOGGER.info('Killing Arakoon process %d', self.process.pid)
                try:
                    self.process.terminate()
                except OSError:
                    LOGGER.exception('Failure while killing Arakoon')

        finally:
            if os.path.isdir(self.base):
                LOGGER.info('Removing tree %s', self.base)
                shutil.rmtree(self.base)

    def _create_client(self):
        client = compat.ArakoonClient(self.client_config)
        client.hello('testsuite', 'pyrakoon_test')
        return client

    def test_hello(self):
        '''Say hello to the Arakoon server'''

        self.assertEquals(
            self._create_client().hello('testsuite', 'pyrakoon_test'),
            'Arakoon "1.2"')

    def test_who_master(self):
        '''Ask who the master is'''

        self.assertEquals(self._create_client().whoMaster(), 'arakoon_0')

    def test_scenario(self):
        '''Run a full-fledged scenario against the test server'''

        client = self._create_client()

        self.assertFalse(client.exists('key'))
        self.assertRaises(compat.ArakoonNotFound, client.get, 'key')
        self.assertRaises(compat.ArakoonNotFound, client.delete, 'key')

        client.set('key', 'value')
        self.assertTrue(client.exists('key'))
        self.assertEquals(client.get('key'), 'value')

        client.delete('key')
        self.assertFalse(client.exists('key'))

        for i in xrange(100):
            client.set('key_%d' % i, 'value')

        matches = client.prefix('key_')
        self.assertEquals(len(matches), 100)

        matches = client.prefix('key_1', 5)
        self.assertEquals(len(matches), 5)

        for match in matches:
            self.assertEquals(client.get(match), 'value')

        matches = client.range('key_10', True, 'key_15', False)
        self.assertEquals(len(matches), 5)
        self.assertEquals(set(matches),
            set('key_%d' % i for i in xrange(10, 15)))

        matches = client.range_entries('key_20', True, 'key_29', True, 20)
        self.assertEquals(len(matches), 10)

        for key, value in matches:
            self.assert_(key.startswith('key_2'))
            self.assertEquals(value, 'value')

        values = client.multiGet(['key_%d' % i for i in xrange(50)])
        self.assertEquals(len(values), 50)
        self.assert_(all(value == 'value' for value in values))

        self.assert_(client.expectProgressPossible())

        self.assertEquals(client.testAndSet('taskey', None, 'value0'), None)
        self.assertEquals(client.testAndSet('taskey', 'value0', 'value1'),
            'value0')
        self.assertEquals(client.testAndSet('taskey', 'value0', 'value2'),
            'value1')
        self.assertEquals(client.testAndSet('taskey', 'value1', None),
            'value1')
        self.assertFalse(client.exists('taskey'))

        sequence = compat.Sequence()
        sequence.addSet('skey', 'value0')
        sequence.addDelete('skey')
        sequence.addSet('skey', 'value1')

        sequence2 = compat.Sequence()
        sequence2.addDelete('skey')
        sequence2.addSet('skey', 'value2')
        sequence.addUpdate(sequence2)

        client.sequence(sequence)

        self.assertEquals(client.get('skey'), 'value2')

        sequence = compat.Sequence()
        sequence.addDelete('skey')
        sequence.addDelete('skey2')

        self.assertRaises(compat.ArakoonNotFound, client.sequence, sequence)

    def test_get_key_count(self):
        client = self._create_client()

        self.assertEqual(client.getKeyCount(), 0)
        client.set('key', 'value')
        print client.getKeyCount()
        self.assertEqual(client.getKeyCount(), 1)
        client.set('key2', 'value')
        self.assertEqual(client.getKeyCount(), 2)

    def test_user_function(self):
        # This is not supported in 'standard' Arakoon
        raise nose.SkipTest

        client = self._create_client()

        s = 'abcdef'
        r = client.userFunction('_arakoon_builtin_reverse', s)
        self.assertEqual(r, s[::-1])

        r = client.userFunction('_arakoon_builtin_reverse', None)
        self.assertEqual(r, None)
