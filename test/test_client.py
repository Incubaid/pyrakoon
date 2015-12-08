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

'''Tests for code in `pyrakoon.client`'''

import unittest

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

try:
    import nose.tools
    HAS_NOSE = True
except ImportError:
    HAS_NOSE = False

from pyrakoon import client, errors, protocol, test, utils
from pyrakoon.client import utils as client_utils

class TestValidateTypes(unittest.TestCase):
    '''Tests for `pyrakoon.client_utils.validate_types`'''

    def test_no_arguments(self):
        '''Test `validate_types` if no arguments are expected'''

        client_utils.validate_types((), ())

    def test_single_correct_argument(self):
        '''Test `validate_types` if one correct argument is provided'''

        client_utils.validate_types((('name', protocol.STRING), ), ('name', ))

    def test_single_incorrect_argument(self):
        '''Test `validate_types` if one incorrect argument is provided'''

        run_test = lambda value: client_utils.validate_types(
            (('i', protocol.UINT32), ), (value, ))

        self.assertRaises(ValueError, run_test, -1)
        self.assertRaises(TypeError, run_test, '1')

    def test_multiple_correct_arguments(self):
        '''Test `validate_types` with multiple correct arguments'''

        client_utils.validate_types((
            ('name', protocol.STRING),
            ('age', protocol.Option(protocol.UINT32)),
        ), ('name', None, ))

    def test_multiple_incorrect_arguments(self):
        '''Test `validate_types` with multiple incorrect arguments'''

        run_test = lambda value: client_utils.validate_types((
            ('name', protocol.STRING),
            ('age', protocol.Option(protocol.UINT32)),
        ), ('name', value, ))

        self.assertRaises(ValueError, run_test, -1)
        self.assertRaises(TypeError, run_test, '1')


class TestClient(unittest.TestCase):
    '''Test the `Client` class'''

    class Client(client.AbstractClient, client.ClientMixin):
        '''A dummy client'''

    def test_invalid_argument_type(self):
        '''Test argument validation of command methods'''

        client_ = self.Client()
        client_.connected = True

        self.assertRaises(TypeError, client_.hello, 123)

    def test_not_connected(self):
        '''Test connection check'''

        client_ = self.Client()

        self.assertRaises(client.NotConnectedError, client_.hello, 'testsuite',
            'pyrakoon_test')
        self.assertRaises(RuntimeError, client_.hello, 'testsuite',
            'pyrakoon_test')


def test_read_blocking():
    '''Test `read_blocking`'''

    data = StringIO.StringIO(''.join(chr(c) for c in (
        0, 0, 0, 0, 3, 0, 0, 0, ord('x'), ord('x'), ord('x'))))

    result = utils.read_blocking(protocol.Hello('testsuite',
        'pyrakoon_test').receive(), data.read)

    if HAS_NOSE:
        nose.tools.assert_equals(result, 'xxx')
    else:
        assert result == 'xxx'


class TestScenario(unittest.TestCase):
    '''Test a more complex scenario using `pyrakoon.test.FakeClient`'''

    def test_scenario(self):
        '''Test a scenario'''

        client_ = test.FakeClient()

        self.assertEquals(client_.hello('testsuite', 'pyrakoon_test'),
            test.FakeClient.VERSION)
        self.assertEquals(client_.who_master(), test.FakeClient.MASTER)

        self.assertFalse(client_.exists('key'))

        client_.set('key', 'value')
        self.assertTrue(client_.exists('key'))

        self.assertEquals(client_.get('key'), 'value')

        client_.delete('key')

        self.assertRaises(errors.NotFound, client_.get, 'key')
        self.assertRaises(errors.NotFound, client_.delete, 'key')
        self.assertFalse(client_.exists('key'))

        for i in xrange(100):
            client_.set('key_%d' % i, 'value_%d' % i)

        self.assertEquals(set(client_.prefix('key_')),
            set('key_%d' % i for i in xrange(100)))

        keys = tuple(client_.prefix('key_1', 5))
        self.assertEquals(len(keys), 5)
        self.assert_(all(key.startswith('key_1') for key in keys))

        self.assertEquals(client_.test_and_set('taskey', None, 'value'), None)
        self.assertEquals(client_.test_and_set('taskey', 'value', 'value2'),
            'value')
        self.assertEquals(client_.test_and_set('taskey', 'value', 'value3'),
            'value2')
        self.assertEquals(client_.get('taskey'), 'value2')

        self.assertEquals(client_.test_and_set('taskey', 'value2', None),
            'value2')

        self.assertFalse(client_.exists('taskey'))
