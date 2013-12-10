# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2013 Incubaid BVBA
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

'''Arakoon administrative call implementations'''

from pyrakoon import protocol, utils

class OptimizeDB(protocol.Message):
    '''"optimize_db" message'''

    __slots__ = ()

    TAG = 0x0025 | protocol.Message.MASK
    ARGS = ()
    RETURN_TYPE = protocol.UNIT

    DOC = utils.format_doc('''
        Send a "optimize_db" command to the server

        This method will trigger optimization of the store on the node this
        command is sent to.

        :note: This only works on slave nodes
    ''')


class DefragDB(protocol.Message):
    '''"defrag_db" message'''

    __slots__ = ()

    TAG = 0x0026 | protocol.Message.MASK
    ARGS = ()
    RETURN_TYPE = protocol.UNIT

    DOC = utils.format_doc('''
        Send a "defrag_db" command to the server

        This method will trigger defragmentation of the store on the node this
        comamand is sent to.

        :note: This only works on slave nodes
    ''')


class DropMaster(protocol.Message):
    '''"drop_master" message'''

    __slots__ = ()

    TAG = 0x0030 | protocol.Message.MASK
    ARGS = ()
    RETURN_TYPE = protocol.UNIT

    DOC = utils.format_doc('''
        Send a "drop_master" command to the server

        This method instructs a node to drop its master role, if possible.
        When the call returns successfully, the node was no longer master, but
        could have gained the master role already in-between.

        :note: This doesn't work in a single-node environment
    ''')
