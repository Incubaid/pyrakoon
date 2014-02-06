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

'''Tests for code in `pyrakoon.sequence`'''

import unittest
import itertools

from pyrakoon import sequence

bytes_ = lambda str_: (ord(c) for c in str_)

class TestSequenceSerialization(unittest.TestCase):
    '''Test serialization of sequences and steps'''

    def test_set_step_serialization(self):
        '''Test serialization of 'set' steps'''

        expected = ''.join(chr(i) for i in itertools.chain(
            (1, 0, 0, 0),
            (3, 0, 0, 0),
            bytes_('key'),
            (5, 0, 0, 0),
            bytes_('value'),
        ))

        received = ''.join(sequence.Set('key', 'value').serialize())

        self.assertEquals(expected, received)

    def test_delete_step_serialization(self):
        '''Test serialization of 'delete' steps'''

        expected = ''.join(chr(i) for i in itertools.chain(
            (2, 0, 0, 0),
            (3, 0, 0, 0),
            bytes_('key'),
        ))

        received = ''.join(sequence.Delete('key').serialize())

        self.assertEquals(expected, received)

    def test_empty_sequence_serialization(self):
        '''Test serialization of an empty sequence'''

        expected = ''.join(chr(i) for i in itertools.chain(
            (5, 0, 0, 0),
            (0, 0, 0, 0),
        ))

        received = ''.join(sequence.Sequence([]).serialize())

        self.assertEquals(expected, received)

    def test_single_step_sequence_serialization(self):
        '''Test serialization of a one-step sequence'''

        expected = ''.join(chr(i) for i in itertools.chain(
            (5, 0, 0, 0),
            (1, 0, 0, 0),
            (1, 0, 0, 0),
            (3, 0, 0, 0),
            bytes_('key'),
            (5, 0, 0, 0),
            bytes_('value'),
        ))

        received = ''.join(
            sequence.Sequence([sequence.Set('key', 'value')]).serialize())

        self.assertEquals(expected, received)

    def test_sequence_serialization(self):
        '''Test serialization of a sequence'''

        expected = ''.join(chr(i) for i in itertools.chain(
            (5, 0, 0, 0),
            (2, 0, 0, 0),
            (1, 0, 0, 0),
            (3, 0, 0, 0),
            bytes_('key'),
            (5, 0, 0, 0),
            bytes_('value'),
            (2, 0, 0, 0),
            (3, 0, 0, 0),
            bytes_('key'),
        ))

        received = ''.join(sequence.Sequence([
            sequence.Set('key', 'value'),
            sequence.Delete('key'),
        ]).serialize())

        self.assertEquals(expected, received)

    def test_nested_sequence_serialization(self):
        '''Test serialization of a nested sequence'''

        expected = ''.join(chr(i) for i in itertools.chain(
            (5, 0, 0, 0), # Sequence
            (3, 0, 0, 0), # 3 steps
            (2, 0, 0, 0), # Delete
            (3, 0, 0, 0),
            bytes_('key'),
            (5, 0, 0, 0), # Sequence
            (2, 0, 0, 0), # 2 steps
            (2, 0, 0, 0), # Delete
            (3, 0, 0, 0),
            bytes_('key'),
            (1, 0, 0, 0), # Set
            (3, 0, 0, 0),
            bytes_('key'),
            (5, 0, 0, 0),
            bytes_('value'),
            (5, 0, 0, 0), # Sequence
            (0, 0, 0, 0), # 0 steps
        ))

        received = ''.join(sequence.Sequence([
            sequence.Delete('key'),
            sequence.Sequence([
                sequence.Delete('key'), sequence.Set('key', 'value')]),
            sequence.Sequence([])]).serialize())

        self.assertEquals(expected, received)
