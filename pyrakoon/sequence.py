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

'''Sequence implementation'''

import operator

from pyrakoon import protocol

#pylint: disable-msg=R0903

class Step(object):
    '''A step in a sequence operation'''

    TAG = None
    '''Operation command tag''' #pylint: disable-msg=W0105
    ARGS = None
    '''Argument definition''' #pylint: disable-msg=W0105

    def __init__(self, *args):
        if len(args) != len(self.ARGS):
            raise TypeError('Invalid number of arguments')

        for (_, type_), arg in zip(self.ARGS, args):
            type_.check(arg)

    def serialize(self):
        '''Serialize the operation

        :return: Serialized operation
        :rtype: iterable of `str`
        '''

        for bytes_ in protocol.UINT32.serialize(self.TAG):
            yield bytes_

        for name, type_ in self.ARGS:
            for bytes_ in type_.serialize(getattr(self, name)):
                yield bytes_


class Set(Step):
    '''"Set" operation'''

    TAG = 1
    ARGS = ('key', protocol.STRING), ('value', protocol.STRING),

    def __init__(self, key, value):
        super(Set, self).__init__(key, value)

        self._key = key
        self._value = value

    key = property(operator.attrgetter('_key'))
    value = property(operator.attrgetter('_value'))

class Delete(Step):
    '''"Delete" operation'''

    TAG = 2
    ARGS = ('key', protocol.STRING),

    def __init__(self, key):
        super(Delete, self).__init__(key)

        self._key = key

    key = property(operator.attrgetter('_key'))

class Assert(Step):
    '''"Assert" operation'''

    TAG = 8
    ARGS = ('key', protocol.STRING), \
        ('value', protocol.Option(protocol.STRING)),

    def __init__(self, key, value):
        super(Assert, self).__init__(key, value)

        self._key = key
        self._value = value

    key = property(operator.attrgetter('_key'))
    value = property(operator.attrgetter('_value'))

class AssertExists(Step):
    '''"AssertExists" operation'''
    TAG = 15
    ARGS = ('key', protocol.STRING),

    def __init__(self,key):
        super(AssertExists,self).__init__(key)
        self._key = key
    
    key = property(operator.attrgetter('_key'))

class Sequence(Step):
    '''"Sequence" operation

    This is a container for a list of other operations.
    '''

    TAG = 5
    ARGS = ()

    def __init__(self, *steps):
        super(Sequence, self).__init__()

        self._steps = tuple(steps)

    steps = property(operator.attrgetter('_steps'))

    def serialize(self):
        for bytes_ in protocol.UINT32.serialize(self.TAG):
            yield bytes_

        for bytes_ in protocol.UINT32.serialize(len(self.steps)):
            yield bytes_

        for step in self.steps:
            for bytes_ in step.serialize():
                yield bytes_
