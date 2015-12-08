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

from lettuce import step, world

try:
    from nose.tools import assert_equals
except ImportError:
    def assert_equals(a, b):
        assert a == b

from pyrakoon import test

@step(u'Given I am connected to Arakoon')
def connect(step):
    world.client = test.FakeClient()

@step(u'I say hello')
def say_hello(step):
    world.result = world.client.hello('pyrakoon_testsuite', 'pyrakoon_test')

@step(u'Then I receive the correct version string')
def validate_version_string(step):
    assert_equals(world.result, test.FakeClient.VERSION)

@step(u'I request the master node name')
def request_master(step):
    world.result = world.client.who_master()

@step(u'Then I receive the correct master node name')
def validate_master(step):
    assert_equals(world.result, test.FakeClient.MASTER)

@step(u'I set "(.*)" to "(.*)"')
def set_key_to_value(step, key, value):
    world.client.set(key.encode('utf-8'), value.encode('utf-8'))

@step(u'I check whether "(.*)" exists')
def check_key_exists(step, key):
    world.result = world.client.exists(key.encode('utf-8'))


@step(u'it should be found')
def validate_found(step):
    assert_equals(world.result, True)

@step(u'it should not be found')
def validate_not_found(step):
    assert_equals(world.result, False)


@step(u'I retrieve "(.*)"')
def retrieve_key(step, key):
    try:
        world.result = world.client.get(key.encode('utf-8'))
    except Exception, ex:
        world.exception = ex

@step(u'(None|".*") should be returned')
def validate_value(step, value):
    if value == 'None':
        value = None
    else:
        value = value.encode('utf-8')[1:-1]

    assert_equals(world.result, value)


@step(u'I delete "(.*)"')
def delete_key(step, key):
    try:
        world.client.delete(key.encode('utf-8'))
    except Exception, exc:
        world.exception = exc


@step(u'Then a (.*) exception is raised')
def validate_exception(step, exc_repr):
    assert_equals(repr(world.exception), exc_repr)


@step(u'I create (\d+) keys starting with "(.*)"')
def create_N_keys_with_prefix(step, count, prefix):
    prefix = prefix.encode('utf-8')

    for i in xrange(int(count)):
        world.client.set('%s%d' % (prefix, i), 'value%d' % i)

@step(u'I retrieve (all|\d+) keys starting with "(.*)"')
def retrieve_all_keys_with_prefix(step, count, prefix):
    prefix = prefix.encode('utf-8')

    if count == 'all':
        world.result = world.client.prefix(prefix)
    else:
        count = int(count)
        world.result = world.client.prefix(prefix, count)

    # We don't want a simple iterable
    world.result = tuple(world.result)

@step(u'Then (\d+) keys should be returned')
def validate_key_count(step, count):
    assert_equals(len(world.result), int(count))


@step(u'I test_and_set "(.*)" from (None|".*") to "(.*)"')
def test_and_set(step, key, test_value, set_value):
    key = key.encode('utf-8')
    test_value = None if test_value == 'None' \
        else test_value.encode('utf-8')[1:-1]
    set_value = set_value.encode('utf-8')

    world.result = world.client.test_and_set(key, test_value, set_value)
