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

'''Tests for code in `pyrakoon.protocol`'''

import random
import inspect
import unittest

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from pyrakoon import errors, protocol, sequence

class TestTypeCheck(unittest.TestCase):
    '''Test `check` method implementations of `Type` classes'''

    def _run_test(self, type_, *values):
        for value, valid in values:
            if valid:
                type_.check(value)
            else:
                self.assertRaises((TypeError, ValueError), type_.check, value)

    def test_string(self):
        '''Test string checks'''

        self._run_test(protocol.STRING,
            ('', True),
            ('abc', True),

            (u'', False),
            (u'abc', False),
            (123, False),
            (None, False)
        )

    def test_unsigned_integer(self):
        '''Test unsigned integer checks'''

        self._run_test(protocol.UINT32,
            (0, True),
            (1, True),
            ((2 ** 32) - 1, True),

            (-1, False),
            (2 ** 32, False),

            ('', False),
            (1.1, False),
            (None, False)
        )

        self._run_test(protocol.UINT64,
            (0, True),
            (1, True),
            ((2 ** 64) - 1, True),

            (-1, False),
            (2 ** 64, False),

            ('', False),
            (1.1, False),
            (None, False)
        )

    def test_signed_integer(self):
        '''Test signed integer checks'''

        self._run_test(protocol.INT32,
            (0, True),
            (1, True),
            (-1, True),
            (((2 ** 32) / 2) - 1, True),
            ((((2 ** 32) / 2) - 1) * (-1), True),

            (((2 ** 32) / 2), False),
            ((((2 ** 32) / 2)) * (-1), False),

            ('', False),
            (1.1, False),
            (None, False)
        )

        self._run_test(protocol.INT64,
            (0, True),
            (1, True),
            (-1, True),
            (((2 ** 64) / 2) - 1, True),
            ((((2 ** 64) / 2) - 1) * (-1), True),

            (((2 ** 64) / 2), False),
            ((((2 ** 64) / 2)) * (-1), False),

            ('', False),
            (1.1, False),
            (None, False)
        )

    def test_bool(self):
        '''Test bool checks'''

        self._run_test(protocol.BOOL,
            (True, True),
            (False, True),

            (0, False),
            ('', False),
            (None, False)
        )

    def test_unit(self):
        '''Test unit checks'''

        self.assertRaises(NotImplementedError, protocol.UNIT.check, object())

    def test_step(self):
        '''Test step checks'''

        self._run_test(protocol.STEP,
            (sequence.Set('key', 'value'), True),
            (sequence.Delete('key'), True),

            (sequence.Sequence([]), True),
            (sequence.Sequence([sequence.Set('key', 'value')]), True),
            (sequence.Sequence([
                sequence.Set('key', 'value'), sequence.Delete('key')]), True),
            (sequence.Sequence([
                sequence.Set('key', 'value'), sequence.Sequence([
                    sequence.Set('key', 'value'), sequence.Delete('key')])]),
                True),

            (object(), False),
            (None, False),
            (sequence.Sequence([sequence.Delete('key'), object()]), False)
        )

    def test_option(self):
        '''Test option checks'''

        type_ = protocol.Option(protocol.STRING)

        self._run_test(type_,
            (None, True),
            ('abc', True),
            ('', True),

            (1, False)
        )

    def test_list(self):
        '''Test list checks'''

        type_ = protocol.List(protocol.STRING)

        self._run_test(type_,
            (('abc',), True),
            (('abc', 'def',), True),
            ((), True),

            ('abc', False),
            ('', False),
            (u'abc', False),
            (u'', False),

            (('abc', None), False)
        )

    def test_product(self):
        '''Test product checks'''

        type_ = protocol.Product(protocol.UINT32, protocol.STRING)

        self._run_test(type_,
            ((0, 'abc'), True),
            ((1, ''), True),

            ((), False),
            (None, False),
            ((-1, 'abc'), False),
            ((0, None), False),
            ('abc', False),
            (u'abc', False),
        )

    def test_complex(self):
        '''Test checking of a complex, nested type'''

        type_ = protocol.Product(
            protocol.UINT32, protocol.List(protocol.STRING),
            protocol.Option(protocol.Product(
                protocol.UINT32, protocol.STRING)))

        self._run_test(type_,
            ((0, ('abc', 'def',), (1, 'abc')), True),
            ((0, ('abc',), None), True),

            ((-1, (), None), False)
        )


class TestTypeSerialization(unittest.TestCase):
    '''
    Test the `serialize` and `receive` methods of `Type` instances are reflexive
    '''

    def _run_test(self, type_, value, handler=None):
        serialized_value = type_.serialize(value)

        data = StringIO.StringIO(''.join(serialized_value))

        receiver = type_.receive()
        request = receiver.next()

        while isinstance(request, protocol.Request):
            request = receiver.send(data.read(request.count))

        self.assert_(isinstance(request, protocol.Result))

        value_ = request.value
        if handler:
            value_ = handler(value_)

        self.assertEqual(value, value_)

    def test_string(self):
        '''Test encoding and decoding of string values'''

        self._run_test(protocol.STRING, 'abcdef')
        self._run_test(protocol.STRING,
            ''.join(chr(random.randint(0, 255)) for _ in xrange(100)))

    def test_unsigned_integer(self):
        '''Test encoding and decoding of unsigned integer values'''

        self._run_test(protocol.UINT32, 0)
        self._run_test(protocol.UINT32, 1)
        self._run_test(protocol.UINT32, (2 ** 32) - 1)

        self._run_test(protocol.UINT64, 0)
        self._run_test(protocol.UINT64, 1)
        self._run_test(protocol.UINT64, (2 ** 64) - 1)

    def test_signed_integer(self):
        '''Test encoding and decoding of signed integer values'''

        self._run_test(protocol.INT32, 0)
        self._run_test(protocol.INT32, 1)
        self._run_test(protocol.INT32, -1)
        self._run_test(protocol.INT32, ((2 ** 32) / 2) - 1)
        self._run_test(protocol.INT32, (((2 ** 32) / 2) - 1) * (-1))

        self._run_test(protocol.INT64, 0)
        self._run_test(protocol.INT64, 1)
        self._run_test(protocol.INT64, -1)
        self._run_test(protocol.INT64, ((2 ** 64) / 2) - 1)
        self._run_test(protocol.INT64, (((2 ** 64) / 2) - 1) * (-1))

    def test_bool(self):
        '''Test encoding and decoding of bool values'''

        self._run_test(protocol.BOOL, True)
        self._run_test(protocol.BOOL, False)

    def test_bool2(self):
        '''Test decoding of invalid bool values'''

        receiver = protocol.BOOL.receive()
        receiver.next()

        self.assertRaises(ValueError,
            receiver.send, protocol.BOOL.PACKER.pack(chr(2)))

    def test_unit(self):
        '''Test encoding and decoding of the unit value'''

        self.assertRaises(NotImplementedError,
            self._run_test, protocol.UNIT, None)

    def test_unit2(self):
        '''Make sure `Unit.receive` results `None`'''

        self.assertEquals(None, protocol.UNIT.receive().next().value)

    def test_step(self):
        '''Test encoding and decoding of Step values'''

        self.assertRaises(NotImplementedError,
            self._run_test, protocol.STEP, sequence.Set('key', 'value'))

    def test_option(self):
        '''Test encoding and decoding of option values'''

        type_ = protocol.Option(protocol.UINT32)

        self._run_test(type_, None)
        self._run_test(type_, 1)

    def test_list(self):
        '''Test encoding and decoding of list values'''

        type_ = protocol.List(protocol.BOOL)

        handle = lambda l: tuple(reversed(l))

        self._run_test(type_, (), handle)
        self._run_test(type_, (True,), handle)
        self._run_test(type_, (True, False,), handle)

    def test_product(self):
        '''Test encoding and decoding of product values'''

        type_ = protocol.Product(protocol.BOOL, protocol.STRING)

        self._run_test(type_, (True, 'abc'))
        self._run_test(type_, (False, ''))

    def test_complex(self):
        '''Test encoding and decoding of a complex type value'''

        type_ = protocol.Product(
            protocol.UINT32, protocol.List(protocol.STRING),
            protocol.Option(protocol.Product(
                protocol.UINT32, protocol.STRING)))

        def handler(value):
            return (value[0], tuple(reversed(value[1])), value[2])

        self._run_test(type_, ((0, ('abc', 'def',), (1, 'abc'))), handler)
        self._run_test(type_, ((0, ('abc',), None)), handler)


class TestExceptions(unittest.TestCase):
    '''Test error code parsing in `Message.receive`'''

    class __metaclass__(type):
        def __new__(cls, name, bases, attrs):
            for name_ in dir(errors):
                attr = getattr(errors, name_)

                if not inspect.isclass(attr) \
                    or not issubclass(attr, errors.ArakoonError) \
                    or attr is errors.ArakoonError:
                    continue

                test_name = 'test_%s' % name_
                assert test_name not in attrs

                attrs[test_name] = cls._generate_test(name_, test_name, attr)

            return type.__new__(cls, name, bases, attrs)

        @staticmethod
        def _generate_test(name, test_name, exception_type):
            def test(self):
                self._run_test(exception_type.CODE, exception_type)

            test.__name__ = test_name
            test.__doc__ = 'Test %s exception parsing' % name

            return test


    def _parse_message(self, message):
        handler = protocol.Message()

        data = StringIO.StringIO(message)

        receiver = handler.receive()
        request = receiver.next()

        while isinstance(request, protocol.Request):
            request = receiver.send(data.read(request.count))

        self.assert_(isinstance(request, protocol.Result))

        return request.value

    def _run_test(self, code, expected_type):
        code_ = ''.join(protocol.UINT32.serialize(code))
        message = ''.join(protocol.STRING.serialize('Exception message'))

        result = '%s%s' % (code_, message)

        self.assertRaises(expected_type, self._parse_message, result)

    def test_unknown_error_code(self):
        '''Test unknown error code handling'''

        code = max(errors.ERROR_MAP.iterkeys()) + 1

        protocol.UINT32.check(code)

        self._run_test(code, errors.ArakoonError)
