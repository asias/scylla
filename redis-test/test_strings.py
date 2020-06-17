#
# Copyright (C) 2019 pengjian.uestc @ gmail.com
#
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
#

import pytest
import redis
import logging
from util import random_string, connect

logger = logging.getLogger('redis-test')

def test_set_get_delete():
    r = connect()
    key = random_string(10)
    val = random_string(10)

    assert r.set(key, val) == True
    assert r.get(key) == val
    assert r.delete(key) == 1
    assert r.get(key) == None 


def test_get():
    r = connect()
    key = random_string(10)
    r.delete(key)

    assert r.get(key) == None 

def test_del_existent_key():
    r = connect()
    key = random_string(10)
    val = random_string(10)

    r.set(key, val)
    assert r.get(key) == val
    assert r.delete(key) == 1

@pytest.mark.xfail(reason="DEL command does not support to return number of deleted keys")
def test_del_non_existent_key():
    r = connect()
    key = random_string(10)
    r.delete(key)
    assert r.delete(key) == 0

def test_set_empty_string():
    r = connect()
    key = random_string(10)
    val = ""
    r.set(key, val)
    assert r.get(key) == val
    r.delete(key)

def test_set_large_string():
    r = connect()
    key = random_string(10)
    val = random_string(4096)
    r.set(key, val)
    assert r.get(key) == val
    r.delete(key)

def test_ping():
    r = connect()
    assert r.ping() == True

def test_echo():
    r = connect()
    assert r.echo('hello world') == 'hello world'

def test_select():
    r = connect()
    key = random_string(10)
    val = random_string(4096)
    r.set(key, val)
    assert r.get(key) == val

    logger.debug('Switch to database 1')
    assert r.execute_command('SELECT 1') == 'OK'
    assert r.get(key) == None

    logger.debug('Switch back to default database 0')
    assert r.execute_command('SELECT 0') == 'OK'
    assert r.get(key) == val
    r.delete(key)
    assert r.get(key) == None

    logger.debug('Try to switch to invalid database 16')
    try:
        r.execute_command('SELECT 16')
        raise Exception('Expect that `SELECT 16` does not work')
    except redis.exceptions.ResponseError as ex:
        assert str(ex) == 'DB index is out of range'
