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

'''Arakoon protocol implementation'''

import struct
import operator
import itertools

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from pyrakoon import utils

# Result codes
RESULT_SUCCESS = 0x0000
'''Success return code''' #pylint: disable-msg=W0105

PROTOCOL_VERSION = 0x00000001
'''Protocol version''' #pylint: disable-msg=W0105


# Wrappers for serialization communication
class Request(object): #pylint: disable-msg=R0903
    '''Wrapper for data requests generated by `Type.receive`'''

    __slots__ = '_count',

    def __init__(self, count):
        self._count = count

    count = property(operator.attrgetter('_count'))

class Result(object): #pylint: disable-msg=R0903
    '''Wrapper for value results generated by `Type.receive`'''

    __slots__ = '_value',

    def __init__(self, value):
        self._value = value

    value = property(operator.attrgetter('_value'))


# Type definitions

class Type(object):
    '''Base type for Arakoon serializable types'''

    PACKER = None
    '''
    `Struct` instance used by default `serialize` and `receive` implementations

    :type: `struct.Struct`
    ''' #pylint: disable-msg=W0105

    def check(self, value):
        '''Check whether a value is valid for this type

        :param value: Value to test
        :type value: `object`

        :return: Whether the value is valid for this type
        :rtype: `bool`
        '''

        raise NotImplementedError

    def serialize(self, value):
        '''Serialize value

        :param value: Value to serialize
        :type value: `object`

        :return: Iterable of bytes of the serialized value
        :rtype: iterable of `str`
        '''

        if not self.PACKER:
            raise NotImplementedError

        yield self.PACKER.pack(value)

    def receive(self):
        '''Receive and parse a result from the server

        This method is a coroutine which yields `Request` instances, and
        finally a `Result`. When a `Request` instance is yield, the number of
        bytes as specified in the *count* attribute should be sent back.

        If finally a `Result` instance is yield, its *value* attribute
        contains the actual message result.

        :see: `Message.receive`
        '''

        if not self.PACKER:
            raise NotImplementedError

        data = yield Request(self.PACKER.size)

        result, = self.PACKER.unpack(data)

        yield Result(result)


class String(Type):
    '''String type'''

    def check(self, value):
        if not isinstance(value, str):
            raise TypeError

    def serialize(self, value):
        length = len(value)

        for bytes_ in UINT32.serialize(length):
            yield bytes_

        yield struct.pack('<%ds' % length, value)

    def receive(self):
        length_receiver = UINT32.receive()
        request = length_receiver.next() #pylint: disable-msg=E1101

        while isinstance(request, Request):
            value = yield request
            request = length_receiver.send(value) #pylint: disable-msg=E1101

        if not isinstance(request, Result):
            raise TypeError

        length = request.value

        if length == 0:
            result = ''
        else:
            data = yield Request(length)
            result, = struct.unpack('<%ds' % length, data)

        yield Result(result)

STRING = String()


class UnsignedInteger(Type):
    '''Unsigned integer type'''

    def __init__(self, bits, pack):
        '''Initialize an unsigned integer type

        :param bits: Bits containing the value
        :type bits: `int`
        :param pack: Struct type, passed to `struct.Struct`
        :type pack: `str`
        '''

        super(UnsignedInteger, self).__init__()

        self.MAX_INT = (2 ** bits) - 1 #pylint: disable-msg=C0103
        self.PACKER = struct.Struct(pack) #pylint: disable-msg=C0103

    def check(self, value):
        if not isinstance(value, (int, long)):
            raise TypeError

        if value < 0:
            raise ValueError('Unsigned integer expected')

        if value > self.MAX_INT:
            raise ValueError('Integer overflow')

UINT32 = UnsignedInteger(32, '<I')
UINT64 = UnsignedInteger(64, '<Q')


class SignedInteger(Type):
    '''Signed integer type'''

    def __init__(self, bits, pack):
        '''Initialize an unsigned integer type

        :param bits: Bits containing the value
        :type bits: `int`
        :param pack: Struct type, passed to `struct.Struct`
        :type pack: `str`
        '''

        super(SignedInteger, self).__init__()

        self.MAX_INT = ((2 ** bits) / 2) - 1 #pylint: disable-msg=C0103
        self.PACKER = struct.Struct(pack) #pylint: disable-msg=C0103

    def check(self, value):
        if not isinstance(value, (int, long)):
            raise TypeError

        if abs(value) > self.MAX_INT:
            raise ValueError('Integer overflow')

INT32 = SignedInteger(32, '<i')
INT64 = SignedInteger(64, '<q')


class Float(Type):
    '''Float type'''

    PACKER = struct.Struct('d')

    def check(self, value):
        if not isinstance(value, float):
            raise TypeError

FLOAT = Float()


class Bool(Type):
    '''Bool type'''

    PACKER = struct.Struct('<c')

    TRUE = chr(1)
    FALSE = chr(0)

    def check(self, value):
        if not isinstance(value, bool):
            raise TypeError

    def serialize(self, value):
        if value:
            yield self.PACKER.pack(self.TRUE)
        else:
            yield self.PACKER.pack(self.FALSE)

    def receive(self):
        value_receiver = super(Bool, self).receive()
        request = value_receiver.next() #pylint: disable-msg=E1101

        while isinstance(request, Request):
            value = yield request
            request = value_receiver.send(value) #pylint: disable-msg=E1101

        if not isinstance(request, Result):
            raise TypeError

        value = request.value

        if value == self.TRUE:
            yield Result(True)
        elif value == self.FALSE:
            yield Result(False)
        else:
            raise ValueError('Unexpected bool value "0x%02x"' % ord(value))

BOOL = Bool()


class Unit(Type): #pylint: disable-msg=R0921
    '''Unit type'''

    def check(self, value):
        raise NotImplementedError('Unit can\'t be checked')

    def serialize(self, value):
        raise NotImplementedError('Unit can\'t be serialized')

    def receive(self):
        yield Result(None)

UNIT = Unit()


class Step(Type):
    '''Step type'''

    def check(self, value):
        from pyrakoon import sequence

        if not isinstance(value, sequence.Step):
            raise TypeError

        if isinstance(value, sequence.Sequence):
            for step in value.steps:
                if not isinstance(step, sequence.Step):
                    raise TypeError

                if isinstance(step, sequence.Sequence):
                    self.check(step)

    def serialize(self, value):
        for part in value.serialize():
            yield part

    def receive(self):
        raise NotImplementedError('Steps can\'t be received')

STEP = Step()


class Option(Type):
    '''Option type'''

    def __init__(self, inner_type):
        super(Option, self).__init__()

        self._inner_type = inner_type

    def check(self, value):
        if value is None:
            return

        self._inner_type.check(value)

    def serialize(self, value):
        if value is None:
            for bytes_ in BOOL.serialize(False):
                yield bytes_
        else:
            for bytes_ in BOOL.serialize(True):
                yield bytes_

            for bytes_ in self._inner_type.serialize(value):
                yield bytes_

    def receive(self):
        has_value_receiver = BOOL.receive()
        request = has_value_receiver.next() #pylint: disable-msg=E1101

        while isinstance(request, Request):
            value = yield request
            request = has_value_receiver.send(value) #pylint: disable-msg=E1101

        if not isinstance(request, Result):
            raise TypeError

        has_value = request.value

        if not has_value:
            yield Result(None)
        else:
            receiver = self._inner_type.receive()
            request = receiver.next() #pylint: disable-msg=E1101

            while isinstance(request, Request):
                value = yield request
                request = receiver.send(value) #pylint: disable-msg=E1101

            if not isinstance(request, Result):
                raise TypeError

            yield Result(request.value)


class List(Type):
    '''List type'''

    def __init__(self, inner_type):
        super(List, self).__init__()

        self._inner_type = inner_type

    def check(self, value):
        # Get rid of the usual suspects
        if isinstance(value, (str, unicode, )):
            raise TypeError

        values = tuple(value)

        for value in values:
            self._inner_type.check(value)

    def serialize(self, value):
        values = tuple(value)

        for bytes_ in UINT32.serialize(len(values)):
            yield bytes_

        for value in values:
            for bytes_ in self._inner_type.serialize(value):
                yield bytes_

    def receive(self):
        count_receiver = UINT32.receive()
        request = count_receiver.next() #pylint: disable-msg=E1101

        while isinstance(request, Request):
            value = yield request
            request = count_receiver.send(value) #pylint: disable-msg=E1101

        if not isinstance(request, Result):
            raise TypeError

        count = request.value

        values = []
        for _ in xrange(count):
            receiver = self._inner_type.receive()
            request = receiver.next() #pylint: disable-msg=E1101

            while isinstance(request, Request):
                value = yield request
                request = receiver.send(value) #pylint: disable-msg=E1101

            if not isinstance(request, Result):
                raise TypeError

            value = request.value

            # Note: can't 'yield' value, otherwise we might not read all values
            # from the stream, and leave it in an unclean state
            values.append(value)

        yield Result(iter(values))


class Product(Type):
    '''Product type'''

    def __init__(self, *inner_types):
        super(Product, self).__init__()

        self._inner_types = tuple(inner_types)

    def check(self, value):
        # Get rid of the usual suspects
        if isinstance(value, (str, unicode, )):
            raise TypeError

        values = tuple(value)

        if len(values) != len(self._inner_types):
            raise ValueError

        for type_, value_ in zip(self._inner_types, values):
            type_.check(value_)

    def serialize(self, value):
        values = tuple(value)

        for type_, value_ in zip(self._inner_types, values):
            for bytes_ in type_.serialize(value_):
                yield bytes_

    def receive(self):
        values = []

        for type_ in self._inner_types:
            receiver = type_.receive()
            request = receiver.next() #pylint: disable-msg=E1101

            while isinstance(request, Request):
                value = yield request
                request = receiver.send(value) #pylint: disable-msg=E1101

            if not isinstance(request, Result):
                raise TypeError

            value = request.value
            values.append(value)

        yield Result(tuple(values))


class StatisticsType(Type):
    '''Statistics type'''

    #pylint: disable-msg=R0912

    def check(self, value):
        raise NotImplementedError('Statistics can\'t be checked')

    def serialize(self, value):
        raise NotImplementedError('Statistics can\'t be serialized')

    def receive(self):
        buffer_receiver = STRING.receive()
        request = buffer_receiver.next() #pylint: disable-msg=E1101

        while isinstance(request, Request):
            value = yield request
            request = buffer_receiver.send(value) #pylint: disable-msg=E1101

        if not isinstance(request, Result):
            raise TypeError

        read = StringIO.StringIO(request.value).read

        class NamedField(Type):
            '''NamedField type'''

            FIELD_TYPE_INT = 1
            FIELD_TYPE_INT64 = 2
            FIELD_TYPE_FLOAT = 3
            FIELD_TYPE_STRING = 4
            FIELD_TYPE_LIST = 5

            def check(self, value):
                raise NotImplementedError('NamedFields can\'t be checked')

            def serialize(self, value):
                raise NotImplementedError('NamedFields can\'t be serialized')

            @classmethod
            def receive(cls):
                type_receiver = INT32.receive()
                request = type_receiver.next() #pylint: disable-msg=E1101

                while isinstance(request, Request):
                    value = yield request
                    #pylint: disable-msg=E1101
                    request = type_receiver.send(value)

                if not isinstance(request, Result):
                    raise TypeError

                type_ = request.value

                name_receiver = STRING.receive()
                request = name_receiver.next() #pylint: disable-msg=E1101

                while isinstance(request, Request):
                    value = yield request
                    #pylint: disable-msg=E1101
                    request = name_receiver.send(value)

                if not isinstance(request, Result):
                    raise TypeError

                name = request.value

                if type_ == cls.FIELD_TYPE_INT:
                    value_receiver = INT32.receive()
                elif type_ == cls.FIELD_TYPE_INT64:
                    value_receiver = INT64.receive()
                elif type_ == cls.FIELD_TYPE_FLOAT:
                    value_receiver = FLOAT.receive()
                elif type_ == cls.FIELD_TYPE_STRING:
                    value_receiver = STRING.receive()
                elif type_ == cls.FIELD_TYPE_LIST:
                    value_receiver = List(NamedField).receive()
                else:
                    raise ValueError('Unknown named field type %d' % type_)

                request = value_receiver.next() #pylint: disable-msg=E1103

                while isinstance(request, Request):
                    value = yield request
                    #pylint: disable-msg=E1103
                    request = value_receiver.send(value)

                if not isinstance(request, Result):
                    raise TypeError

                value = request.value

                if type_ == cls.FIELD_TYPE_LIST:
                    result = dict()
                    map(result.update, value) #pylint: disable-msg=W0141
                    value = result

                yield Result({name: value})

        result = utils.read_blocking(NamedField.receive(), read)

        if 'arakoon_stats' not in result:
            raise ValueError('Missing expected \'arakoon_stats\' value')

        yield Result(result['arakoon_stats'])

STATISTICS = StatisticsType()


# Protocol message definitions

class Message(object):
    '''Base type for Arakoon command messages'''

    MASK = 0xb1ff0000
    '''Generic command mask value''' #pylint: disable-msg=W0105

    TAG = None
    '''Tag (code) of the command''' #pylint: disable-msg=W0105
    ARGS = None
    '''Arguments required for the command''' #pylint: disable-msg=W0105
    RETURN_TYPE = None
    '''Return type of the command''' #pylint: disable-msg=W0105
    DOC = None
    '''Docstring for methods exposing this command''' #pylint: disable-msg=W0105
    HAS_ALLOW_DIRTY = False
    '''Marker whether the command has an 'allow dirty' flag''' #pylint: disable-msg=W0105, C0301

    def serialize(self):
        '''Serialize the command

        :return: Iterable of bytes of the serialized version of the command
        :rtype: iterable of `str`
        '''

        for bytes_ in UINT32.serialize(self.TAG):
            yield bytes_

        # TODO: Hack -> never allow dirty reads, for now
        if self.HAS_ALLOW_DIRTY:
            for bytes_ in BOOL.serialize(False):
                yield bytes_

        for arg in self.ARGS:
            if len(arg) == 2:
                name, type_ = arg
            elif len(arg) == 3:
                name, type_, _ = arg
            else:
                raise ValueError

            for bytes_ in type_.serialize(getattr(self, name)):
                yield bytes_

    def receive(self):
        '''Read and deserialize the return value of the command

        Running as a coroutine, this method can read and parse the server
        result value once this command has been submitted.

        This method yields values of type `Request` to request more data (which
        should then be injected using the *send* method of the coroutine). The
        number of requested bytes is provided in the *count* attribute of the
        `Request` object.

        Finally a `Result` value is generated, which contains the server result
        in its *value* attribute.

        :raise ArakoonError: Server returned an error code

        :see: `pyrakoon.utils.process_blocking`
        '''

        from pyrakoon import errors

        code_receiver = UINT32.receive()
        request = code_receiver.next() #pylint: disable-msg=E1101

        while isinstance(request, Request):
            value = yield request
            request = code_receiver.send(value) #pylint: disable-msg=E1101

        if not isinstance(request, Result):
            raise TypeError

        code = request.value

        if code == RESULT_SUCCESS:
            result_receiver = self.RETURN_TYPE.receive()
        else:
            # Error
            result_receiver = STRING.receive()

        request = result_receiver.next() #pylint: disable-msg=E1103

        while isinstance(request, Request):
            value = yield request
            request = result_receiver.send(value) #pylint: disable-msg=E1103

        if not isinstance(request, Result):
            raise TypeError

        result = request.value

        if code == RESULT_SUCCESS:
            yield Result(result)
        else:
            if code in errors.ERROR_MAP:
                raise errors.ERROR_MAP[code](result)
            else:
                raise errors.ArakoonError(
                    'Unknown error code 0x%x, server said: %s' % \
                        (code, result))


class Hello(Message):
    '''"hello" message'''

    __slots__ = '_client_id', '_cluster_id',

    TAG = 0x0001 | Message.MASK
    ARGS = ('client_id', STRING), ('cluster_id', STRING),
    RETURN_TYPE = STRING

    DOC = utils.format_doc('''
        Send a "hello" command to the server

        This method will return the string returned by the server when
        receiving a "hello" command.

        :param client_id: Identifier of the client
        :type client_id: `str`
        :param cluster_id: Identifier of the cluster connecting to
            This must match the cluster configuration.
        :type cluster_id: `str`

        :return: Message returned by the server
        :rtype: `str`
    ''')

    def __init__(self, client_id, cluster_id):
        super(Hello, self).__init__()

        self._client_id = client_id
        self._cluster_id = cluster_id

    client_id = property(operator.attrgetter('_client_id'))
    cluster_id = property(operator.attrgetter('_cluster_id'))


class WhoMaster(Message):
    '''"who_master" message'''

    __slots__ = ()

    TAG = 0x0002 | Message.MASK
    ARGS = ()
    RETURN_TYPE = Option(STRING)

    DOC = utils.format_doc('''
        Send a "who_master" command to the server

        This method returns the name of the current master node in the Arakoon
        cluster.

        :return: Name of cluster master node
        :rtype: `str`
    ''')


class Exists(Message):
    '''"exists" message'''

    __slots__ = '_key',

    TAG = 0x0007 | Message.MASK
    ARGS = ('key', STRING),
    RETURN_TYPE = BOOL
    HAS_ALLOW_DIRTY = True

    DOC = utils.format_doc('''
        Send an "exists" command to the server

        This method returns a boolean which tells whether the given `key` is
        set on the server.

        :param key: Key to test
        :type key: `str`

        :return: Whether the given key is set on the server
        :rtype: `bool`
    ''')

    def __init__(self, key):
        super(Exists, self).__init__()

        self._key = key

    key = property(operator.attrgetter('_key'))


class Get(Message):
    '''"get" message'''

    __slots__ = '_key',

    TAG = 0x0008 | Message.MASK
    ARGS = ('key', STRING),
    RETURN_TYPE = STRING
    HAS_ALLOW_DIRTY = True

    DOC = utils.format_doc('''
        Send a "get" command to the server

        This method returns the value of the requested key.

        :param key: Key to retrieve
        :type key: `str`

        :return: Value for the given key
        :rtype: `str`
    ''')

    def __init__(self, key):
        super(Get, self).__init__()

        self._key = key

    key = property(operator.attrgetter('_key'))


class Set(Message):
    '''"set" message'''

    __slots__ = '_key', '_value',

    TAG = 0x0009 | Message.MASK
    ARGS = ('key', STRING), ('value', STRING),
    RETURN_TYPE = UNIT

    DOC = utils.format_doc('''
        Send a "set" command to the server

        This method sets a given key to a given value on the server.

        :param key: Key to set
        :type key: `str`
        :param value: Value to set
        :type value: `str`
    ''')

    def __init__(self, key, value):
        super(Set, self).__init__()

        self._key = key
        self._value = value

    key = property(operator.attrgetter('_key'))
    value = property(operator.attrgetter('_value'))


class Delete(Message):
    '''"delete" message'''

    __slots__ = '_key',

    TAG = 0x000a | Message.MASK
    ARGS = ('key', STRING),
    RETURN_TYPE = UNIT

    DOC = utils.format_doc('''
        Send a "delete" command to the server

        This method deletes a given key from the cluster.

        :param key: Key to delete
        :type key: `str`
    ''')

    def __init__(self, key):
        super(Delete, self).__init__()

        self._key = key

    key = property(operator.attrgetter('_key'))


class PrefixKeys(Message):
    '''"prefix_keys" message'''

    __slots__ = '_prefix', '_max_elements',

    TAG = 0x000c | Message.MASK
    ARGS = ('prefix', STRING), ('max_elements', INT32, -1),
    RETURN_TYPE = List(STRING)
    HAS_ALLOW_DIRTY = True

    DOC = utils.format_doc('''
        Send a "prefix_keys" command to the server

        This method retrieves a list of keys from the cluster matching a given
        prefix. A maximum number of returned keys can be provided. If set to -1
        (the default), all matching keys will be returned.

        :param prefix: Prefix to match
        :type prefix: `str`
        :param max_elements: Maximum number of keys to return
        :type max_elements: `int`

        :return: Keys matching the given prefix
        :rtype: iterable of `str`
    ''')

    def __init__(self, prefix, max_elements):
        super(PrefixKeys, self).__init__()

        self._prefix = prefix
        self._max_elements = max_elements

    prefix = property(operator.attrgetter('_prefix'))
    max_elements = property(operator.attrgetter('_max_elements'))


class TestAndSet(Message):
    '''"test_and_set" message'''

    __slots__ = '_key', '_test_value', '_set_value',

    TAG = 0x000d | Message.MASK
    ARGS = ('key', STRING), ('test_value', Option(STRING)), \
        ('set_value', Option(STRING)),
    RETURN_TYPE = Option(STRING)

    DOC = utils.format_doc('''
        Send a "test_and_set" command to the server

        When `test_value` is not `None`, the value for `key` will only be
        modified if the existing value on the server is equal to `test_value`.
        When `test_value` is `None`, the `key` will only be set of there was no
        value set for the `key` before.

        When `set_value` is `None`, the `key` will be deleted on the server.

        The original value for `key` is returned.

        :param key: Key to act on
        :type key: `str`
        :param test_value: Expected value to test for
        :type test_value: `str` or `None`
        :param set_value: New value to set
        :type set_value: `str` or `None`

        :return: Original value of `key`
        :rtype: `str`
    ''')

    def __init__(self, key, test_value, set_value):
        super(TestAndSet, self).__init__()

        self._key = key
        self._test_value = test_value
        self._set_value = set_value

    key = property(operator.attrgetter('_key'))
    test_value = property(operator.attrgetter('_test_value'))
    set_value = property(operator.attrgetter('_set_value'))


class Sequence(Message):
    '''"sequence" and "synced_sequence" message'''

    __slots__ = '_steps', '_sync',

    ARGS = ('steps', List(STEP)), ('sync', BOOL, False),
    RETURN_TYPE = UNIT

    DOC = utils.format_doc('''
        Send a "sequence" or "synced_sequence" command to the server

        The operations passed to the constructor should be instances of
        implementations of the `pyrakoon.sequence.Step` class. These operations
        will be executed in an all-or-nothing transaction.

        :param steps: Steps to execute
        :type steps: iterable of `pyrakoon.sequence.Step`
        :param sync: Use `synced_sequence`
        :type sync: `bool`
    ''')

    def __init__(self, steps, sync):
        from pyrakoon import sequence

        super(Sequence, self).__init__()

        #pylint: disable-msg=W0142
        if len(steps) == 1 and isinstance(steps[0], sequence.Sequence):
            self._sequence = steps[0]
        else:
            self._sequence = sequence.Sequence(*steps)

        self._sync = sync

    sequence = property(operator.attrgetter('_sequence'))
    sync = property(operator.attrgetter('_sync'))

    def serialize(self):
        tag = (0x0010 if not self.sync else 0x0024) | Message.MASK

        for bytes_ in UINT32.serialize(tag):
            yield bytes_

        sequence_bytes = ''.join(self.sequence.serialize())

        for bytes_ in STRING.serialize(sequence_bytes):
            yield bytes_


class Range(Message):
    '''"Range" message'''

    __slots__ = '_begin_key', '_begin_inclusive', '_end_key', \
        '_end_inclusive', '_max_elements',

    TAG = 0x000b | Message.MASK
    ARGS = ('begin_key', Option(STRING)), ('begin_inclusive', BOOL), \
        ('end_key', Option(STRING)), ('end_inclusive', BOOL), \
        ('max_elements', INT32, -1),
    RETURN_TYPE = List(STRING)
    HAS_ALLOW_DIRTY = True

    DOC = utils.format_doc('''
        Send a "range" command to the server

        The operation will return a list of keys, in the range between
        `begin_key` and `end_key`. The `begin_inclusive` and `end_inclusive`
        flags denote whether the delimiters should be included.

        The `max_elements` flag can limit the number of returned keys. If it is
        negative, all matching keys are returned.

        :param begin_key: Begin of range
        :type begin_key: `str`
        :param begin_inclusive: `begin_key` is in- or exclusive
        :type begin_inclusive: `bool`
        :param end_key: End of range
        :type end_key: `str`
        :param end_inclusive: `end_key` is in- or exclusive
        :param max_elements: Maximum number of keys to return
        :type max_elements: `int`

        :return: List of matching keys
        :rtype: iterable of `str`
    ''')

    #pylint: disable-msg=R0913
    def __init__(self, begin_key, begin_inclusive, end_key, end_inclusive,
        max_elements):
        super(Range, self).__init__()

        self._begin_key = begin_key
        self._begin_inclusive = begin_inclusive
        self._end_key = end_key
        self._end_inclusive = end_inclusive
        self._max_elements = max_elements

    begin_key = property(operator.attrgetter('_begin_key'))
    begin_inclusive = property(operator.attrgetter('_begin_inclusive'))
    end_key = property(operator.attrgetter('_end_key'))
    end_inclusive = property(operator.attrgetter('_end_inclusive'))
    max_elements = property(operator.attrgetter('_max_elements'))


class RangeEntries(Message):
    '''"RangeEntries" message'''

    __slots__ = '_begin_key', '_begin_inclusive', '_end_key', \
        '_end_inclusive', '_max_elements',

    TAG = 0x000f | Message.MASK
    ARGS = ('begin_key', Option(STRING)), ('begin_inclusive', BOOL), \
        ('end_key', Option(STRING)), ('end_inclusive', BOOL), \
        ('max_elements', INT32, -1),
    RETURN_TYPE = List(Product(STRING, STRING))
    HAS_ALLOW_DIRTY = True

    DOC = utils.format_doc('''
        Send a "range_entries" command to the server

        The operation will return a list of (key, value) tuples, for keys in the
        range between `begin_key` and `end_key`. The `begin_inclusive` and
        `end_inclusive` flags denote whether the delimiters should be included.

        The `max_elements` flag can limit the number of returned items. If it is
        negative, all matching items are returned.

        :param begin_key: Begin of range
        :type begin_key: `str`
        :param begin_inclusive: `begin_key` is in- or exclusive
        :type begin_inclusive: `bool`
        :param end_key: End of range
        :type end_key: `str`
        :param end_inclusive: `end_key` is in- or exclusive
        :param max_elements: Maximum number of items to return
        :type max_elements: `int`

        :return: List of matching (key, value) pairs
        :rtype: iterable of (`str`, `str`)
    ''')

    #pylint: disable-msg=R0913
    def __init__(self, begin_key, begin_inclusive, end_key, end_inclusive,
        max_elements):
        super(RangeEntries, self).__init__()

        self._begin_key = begin_key
        self._begin_inclusive = begin_inclusive
        self._end_key = end_key
        self._end_inclusive = end_inclusive
        self._max_elements = max_elements

    begin_key = property(operator.attrgetter('_begin_key'))
    begin_inclusive = property(operator.attrgetter('_begin_inclusive'))
    end_key = property(operator.attrgetter('_end_key'))
    end_inclusive = property(operator.attrgetter('_end_inclusive'))
    max_elements = property(operator.attrgetter('_max_elements'))


class MultiGet(Message):
    '''"multi_get" message'''

    __slots__ = '_keys',

    TAG = 0x0011 | Message.MASK
    ARGS = ('keys', List(STRING)),
    RETURN_TYPE = List(STRING)
    HAS_ALLOW_DIRTY = True

    DOC = utils.format_doc('''
        Send a "multi_get" command to the server

        This method returns a list of the values for all requested keys.

        :param keys: Keys to look up
        :type keys: iterable of `str`

        :return: Requested values
        :rtype: iterable of `str`
    ''')

    def __init__(self, keys):
        super(MultiGet, self).__init__()

        self._keys = keys

    keys = property(operator.attrgetter('_keys'))


class ExpectProgressPossible(Message):
    '''"expect_progress_possible" message'''

    __slots__ = ()

    TAG = 0x0012 | Message.MASK
    ARGS = ()
    RETURN_TYPE = BOOL

    DOC = utils.format_doc('''
        Send a "expect_progress_possible" command to the server

        This method returns whether the master thinks progress is possible.

        :return: Whether the master thinks progress is possible
        :rtype: `bool`
    ''')


class GetKeyCount(Message):
    '''"get_key_count" message'''

    __slots__ = ()

    TAG = 0x001a | Message.MASK
    ARGS = ()
    RETURN_TYPE = UINT64

    DOC = utils.format_doc('''
        Send a "get_key_count" command to the server

        This method returns the number of items stored in Arakoon.

        :return: Number of items stored in the database
        :rtype: `int`
    ''')


class UserFunction(Message):
    '''"user_function" message'''

    __slots__ = '_function', '_arg',

    TAG = 0x0015 | Message.MASK
    ARGS = ('function', STRING), ('argument', Option(STRING)),
    RETURN_TYPE = Option(STRING)

    DOC = utils.format_doc('''
        Send a "user_function" command to the server

        This method returns the result of the function invocation.

        :param function: Name of the user function to invoke
        :type function: `str`
        :param argument: Argument to pass to the function
        :type argument: `str` or `None`

        :return: Result of the function invocation
        :rtype: `str` or `None`
    ''')

    def __init__(self, function, argument):
        super(UserFunction, self).__init__()

        self._function = function
        self._argument = argument

    function = property(operator.attrgetter('_function'))
    argument = property(operator.attrgetter('_argument'))


class Confirm(Message):
    '''"confirm" message'''

    __slots__ = '_key', '_value',

    TAG = 0x001c | Message.MASK
    ARGS = ('key', STRING), ('value', STRING),
    RETURN_TYPE = UNIT

    DOC = utils.format_doc('''
        Send a "confirm" command to the server

        This method sets a given key to a given value on the server, unless
        the value bound to the key is already equal to the provided value, in
        which case the action becomes a no-op.

        :param key: Key to set
        :type key: `str`
        :param value: Value to set
        :type value: `str`
    ''')

    def __init__(self, key, value):
        super(Confirm, self).__init__()

        self._key = key
        self._value = value

    key = property(operator.attrgetter('_key'))
    value = property(operator.attrgetter('_value'))


class Assert(Message):
    '''"assert" message'''

    __slots__ = '_key', '_value',

    TAG = 0x0016 | Message.MASK
    ARGS = ('key', STRING), ('value', Option(STRING)),
    RETURN_TYPE = UNIT
    HAS_ALLOW_DIRTY = True

    DOC = utils.format_doc('''
        Send an 'assert' command to the server

        `assert key vo` throws an exception if the value associated with the
        key is not what was expected.

        :param key: Key to check
        :type key: `str`
        :param value: Optional value to compare
        :type value: `str` or `None`
    ''')

    def __init__(self, key, value):
        super(Assert, self).__init__()

        self._key = key
        self._value = value

    key = property(operator.attrgetter('_key'))
    value = property(operator.attrgetter('_value'))


class RevRangeEntries(Message):
    '''"rev_range_entries" message'''

    __slots__ = '_begin_key', '_begin_inclusive', '_end_key', \
        '_end_inclusive', '_max_elements',

    TAG = 0x0023 | Message.MASK
    ARGS = ('begin_key', Option(STRING)), ('begin_inclusive', BOOL), \
        ('end_key', Option(STRING)), ('end_inclusive', BOOL), \
        ('max_elements', INT32, -1),
    RETURN_TYPE = List(Product(STRING, STRING))
    HAS_ALLOW_DIRTY = True

    DOC = utils.format_doc('''
        Send a "rev_range_entries" command to the server

        The operation will return a list of (key, value) tuples, for keys in
        the reverse range between `begin_key` and `end_key`. The
        `begin_inclusive` and `end_inclusive` flags denote whether the
        delimiters should be included.

        The `max_elements` flag can limit the number of returned items. If it is
        negative, all matching items are returned.

        :param begin_key: Begin of range
        :type begin_key: `str`
        :param begin_inclusive: `begin_key` is in- or exclusive
        :type begin_inclusive: `bool`
        :param end_key: End of range
        :type end_key: `str`
        :param end_inclusive: `end_key` is in- or exclusive
        :param max_elements: Maximum number of items to return
        :type max_elements: `int`

        :return: List of matching (key, value) pairs
        :rtype: iterable of (`str`, `str`)
    ''')

    #pylint: disable-msg=R0913
    def __init__(self, begin_key, begin_inclusive, end_key, end_inclusive,
        max_elements):
        super(RevRangeEntries, self).__init__()

        self._begin_key = begin_key
        self._begin_inclusive = begin_inclusive
        self._end_key = end_key
        self._end_inclusive = end_inclusive
        self._max_elements = max_elements

    begin_key = property(operator.attrgetter('_begin_key'))
    begin_inclusive = property(operator.attrgetter('_begin_inclusive'))
    end_key = property(operator.attrgetter('_end_key'))
    end_inclusive = property(operator.attrgetter('_end_inclusive'))
    max_elements = property(operator.attrgetter('_max_elements'))


class Statistics(Message):
    '''"statistics" message'''

    __slots__ = ()

    TAG = 0x0013 | Message.MASK
    ARGS = ()
    RETURN_TYPE = STATISTICS

    DOC = utils.format_doc('''
        Send a "statistics" command to the server

        This method returns some server statistics.

        :return: Server statistics
        :rtype: `Statistics`
    ''')


def build_prologue(cluster):
    '''Return the string to send as prologue

    :param cluster: Name of the cluster to which a connection is made
    :type cluster: `str`

    :return: Prologue to send to the Arakoon server
    :rtype: `str`
    '''

    return ''.join(itertools.chain(
        UINT32.serialize(Message.MASK),
        UINT32.serialize(PROTOCOL_VERSION),
        STRING.serialize(cluster)))
