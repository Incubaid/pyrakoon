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

'''Tests for code in `pyrakoon.nursery`'''

import logging
import unittest

from twisted.internet import defer, error, interfaces, protocol, reactor
import twisted.trial.unittest

from pyrakoon import client, compat, nursery, test, tx

LOGGER = logging.getLogger(__name__)

CONFIG_TEMPLATE = '''
[global]
cluster = arakoon_0
cluster_id = %(CLUSTER_ID)s

[nursery]
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

class TestNurseryClient(unittest.TestCase, test.NurseryEnvironmentMixin):
    '''Test the nursery client against a real Arakoon nursery setup'''

    def setUp(self):
        client_config, config_path, base = self.setUpNursery(
            'pyrakoon_test_nursery', CONFIG_TEMPLATE)
        self.client_config = compat.ArakoonClientConfig(*client_config)

    def tearDown(self):
        self.tearDownNursery()

    def _create_client(self):
        client = compat.ArakoonClient(self.client_config)
        client.hello('testsuite', self.client_config.getClusterId())
        return client

    def _create_nursery_client(self):
        def morph(name, cluster_info):
            cluster_info2 = {}

            for node_id, (ips, port) in cluster_info.iteritems():
                cluster_info2[node_id] = (ips, port)

            return compat.ArakoonClient(
                compat.ArakoonClientConfig(name, cluster_info2))

        return nursery.NurseryClient(self._create_client()._client._process,
            morph)

    def test_scenario(self):
        client = self._create_nursery_client()

        client.set('key', 'value')
        self.assertEqual(client.get('key'), 'value')
        client.delete('key')


class ArakoonClientProtocol(tx.ArakoonProtocol, client.ClientMixin):
    '''Twisted Arakoon client protocol'''


class TestNurseryClientTx(twisted.trial.unittest.TestCase,
    test.NurseryEnvironmentMixin):
    '''Test Twisted code against an Arakoon nursery setup'''

    CLUSTER_ID = 'pyrakoon_test_nursery_tx'

    def setUp(self):
        client_config, _, _2 = self.setUpNursery(
            self.CLUSTER_ID, CONFIG_TEMPLATE)
        self.client_config = client_config

    def tearDown(self):
        self.tearDownNursery()

    @defer.inlineCallbacks
    def _create_client(self):
        client = protocol.ClientCreator(reactor,
            ArakoonClientProtocol, self.CLUSTER_ID)

        cluster_id, nodes = self.client_config
        ips, port = nodes.values()[0]

        proto = yield client.connectTCP(ips[0], port)

        try:
            yield proto.hello('test_nursery_client_tx', cluster_id)
        except:
            proto.transport.loseConnection()
            raise

        defer.returnValue(proto)

    @defer.inlineCallbacks
    def test_get_nursery_config(self):
        proto = yield self._create_client()

        try:
            message = nursery.GetNurseryConfig()
            config = yield proto._process(message)

            self.assertEqual(config.routing, nursery.LeafNode(self.CLUSTER_ID))
            self.assert_(self.CLUSTER_ID in config.clusters)

        finally:
            proto.transport.loseConnection()
