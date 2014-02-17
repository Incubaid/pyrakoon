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

import time
import logging
import unittest

import nose

from pyrakoon import compat, sequence, test

LOGGER = logging.getLogger(__name__)

CONFIG_TEMPLATE = '''
[global]
cluster = arakoon_0
cluster_id = %(CLUSTER_ID)s

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


class TestCompatClient(unittest.TestCase, test.ArakoonEnvironmentMixin):
    '''Test the compatibility client against a real Arakoon server'''

    def setUp(self):
        config, _, _2 = self.setUpArakoon('pyrakoon_test_compat',
            CONFIG_TEMPLATE)

        self.client_config = compat.ArakoonClientConfig(*config)

        # Give server some time to get up
        ok = False
        for _ in xrange(5):
            LOGGER.info('Attempting hello call')
            try:
                client = self._create_client()
                client.hello('testsuite', config[0])
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
        self.tearDownArakoon()

    def _create_client(self):
        client = compat.ArakoonClient(self.client_config)
        client.hello('testsuite', self.client_config.getClusterId())
        return client

    def test_hello(self):
        '''Say hello to the Arakoon server'''

        response = self._create_client().hello(
            'testsuite', self.client_config.getClusterId())

        self.assert_('Arakoon' in response)

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

        sequence_ = compat.Sequence()
        sequence_.addSet('skey', 'value0')
        sequence_.addDelete('skey')
        sequence_.addSet('skey', 'value1')

        sequence2 = compat.Sequence()
        sequence2.addDelete('skey')
        sequence2.addSet('skey', 'value2')
        sequence2.addAssert('skey', 'value2')
        sequence2.addAssert('skey2', None)
        sequence_.addUpdate(sequence2)

        client.sequence(sequence_)
        client.sequence(sequence_, sync=False)
        client.sequence(sequence_, sync=True)

        self.assertEquals(client.get('skey'), 'value2')

        sequence_ = compat.Sequence()
        sequence_.addDelete('skey')
        sequence_.addDelete('skey2')

        self.assertRaises(compat.ArakoonNotFound, client.sequence, sequence_)

        # This is a bit out-of-place, but it's the only occasion where
        # sequence-with-a-list can be tested easily
        client._client.sequence([sequence.Set('skey', 'svalue3')])
        self.assertEquals(client.get('skey'), 'svalue3')

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

    def test_confirm(self):
        client = self._create_client()

        k = 'key'
        v1 = 'value1'
        v2 = 'value2'

        client.set(k, v1)
        self.assertEqual(client.get(k), v1)
        client.confirm(k, v1)
        self.assertEqual(client.get(k), v1)
        client.confirm(k, v2)
        self.assertEqual(client.get(k), v2)

    def test_assert(self):
        client = self._create_client()

        k = 'key'
        v1 = 'value1'
        v2 = 'value2'

        client.aSSert(k, None)
        self.assertRaises(compat.ArakoonAssertionFailed, client.aSSert, k, v1)
        client.set(k, v1)
        client.aSSert(k, v1)
        self.assertRaises(compat.ArakoonAssertionFailed, client.aSSert, k, None)
        self.assertRaises(compat.ArakoonAssertionFailed, client.aSSert, k, v2)

    def test_rev_range_entries(self):
        client = self._create_client()

        for i in xrange(100):
            client.set('key_%d' % i, 'value_%d' % i)

        result = client.rev_range_entries('key_90', True, 'key_80', False, 5)

        self.assertEquals(result,
            [('key_90', 'value_90'), ('key_9', 'value_9'),
             ('key_89', 'value_89'), ('key_88', 'value_88'),
             ('key_87', 'value_87')])

    def test_statistics(self):
        client = self._create_client()

        statistics = client.statistics()

        self.assert_('start' in statistics)

    def test_version(self):
        client = self._create_client()

        version = client.getVersion()

        self.assert_(len(version) == 4)
        self.assert_(isinstance(version[0], int))
        self.assert_(isinstance(version[1], int))
        self.assert_(isinstance(version[2], int))
        self.assert_(isinstance(version[3], str))
        self.assertEqual(version[0], 1)
