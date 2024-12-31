#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import logging
import pytest
import time
import asyncio
import os

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


async def get_injection_params(manager, node_ip, injection):
    res = await manager.api.get_injection(node_ip, injection)
    logger.debug(f"get_injection_params({injection}): {res}")
    assert len(res) == 1
    shard_res = res[0]
    assert shard_res["enabled"]
    if "parameters" in shard_res:
        return {item["key"]: item["value"] for item in shard_res["parameters"]}
    else:
        return {}


@pytest.mark.skip(reason="test")
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_enable_compacting_data_for_streaming_and_repair_live_update(manager):
    """
    Check that enable_compacting_data_for_streaming_and_repair is live_update.
    This config item has a non-trivial path of propagation and live-update was
    silently broken in the past.
    """
    cmdline = ["--enable-compacting-data-for-streaming-and-repair", "0", "--smp", "1", "--logger-log-level", "api=trace"]
    node1 = await manager.server_add(cmdline=cmdline)
    node2 = await manager.server_add(cmdline=cmdline)

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY)")

    config_item = "enable_compacting_data_for_streaming_and_repair"

    host1, host2 = await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 30)

    for host in (host1, host2):
        res = list(cql.execute(f"SELECT value FROM system.config WHERE name = '{config_item}'", host=host))
        assert res[0].value == "false"

    await manager.api.enable_injection(node1.ip_addr, "maybe_compact_for_streaming", False, {})

    # Before the first repair, there should be no parameters present
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming")) == {}

    # After the initial repair, we should see the config item value matching the value set via the command-line.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming"))["compaction_enabled"] == "false"

    for host in (host1, host2):
        cql.execute(f"UPDATE system.config SET value = '1' WHERE name = '{config_item}'", host=host)

    # After the update to the config above, the next repair should pick up the updated value.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming"))["compaction_enabled"] == "true"


@pytest.mark.skip(reason="test")
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tombstone_gc_for_streaming_and_repair(manager):
    """
    Check that:
    * enable_tombstone_gc_for_streaming_and_repair=1 works as expected
    * enable_tombstone_gc_for_streaming_and_repair=0 works as expected
    * enable_tombstone_gc_for_streaming_and_repair is live-update
    """
    cmdline = [
            "--enable-compacting-data-for-streaming-and-repair", "1",
            "--enable-tombstone-gc-for-streaming-and-repair", "1",
            "--enable-cache", "0",
            "--hinted-handoff-enabled", "0",
            "--smp", "1",
            "--logger-log-level", "api=trace:database=trace"]
    node1 = await manager.server_add(cmdline=cmdline)
    node2 = await manager.server_add(cmdline=cmdline)

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH compaction = {'class': 'NullCompactionStrategy'}")

    await manager.server_stop_gracefully(node2.server_id)

    stmt = SimpleStatement("DELETE FROM ks.tbl WHERE pk = 0 AND ck = 0", consistency_level=ConsistencyLevel.ONE)
    cql.execute(stmt)

    await manager.server_start(node2.server_id, wait_others=1)

    # Flush memtables and remove commitlog, so we can freely GC tombstones.
    await manager.server_restart(node1.server_id, wait_others=1)

    host1, host2 = await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 30)

    config_item = "enable_tombstone_gc_for_streaming_and_repair"

    def check_nodes_have_data(node1_has_data, node2_has_data):
        for (host, host_has_data) in ((host1, node1_has_data), (host2, node2_has_data)):
            res = list(cql.execute("SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 0", host=host))
            print(res)
            if host_has_data:
                assert len(res) == 3
            else:
                assert len(res) < 3

    # Initial start-condition check
    check_nodes_have_data(True, False)

    await manager.api.enable_injection(node1.ip_addr, "maybe_compact_for_streaming", False, {})

    # Make the tombstone purgeable
    cql.execute("ALTER TABLE ks.tbl WITH tombstone_gc = {'mode': 'immediate'}")

    # With enable_tombstone_gc_for_streaming_and_repair=1, repair
    # should not find any differences and thus not replicate the GCable
    # tombstone.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming")) == {
            "compaction_enabled": "true", "compaction_can_gc": "true"}
    check_nodes_have_data(True, False)

    for host in (host1, host2):
        cql.execute(f"UPDATE system.config SET value = '0' WHERE name = '{config_item}'", host=host)

    # With enable_tombstone_gc_for_streaming_and_repair=0, repair
    # should find the differences and replicate the GCable tombstone.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming")) == {
            "compaction_enabled": "true", "compaction_can_gc": "false"}
    check_nodes_have_data(True, True)

@pytest.mark.skip(reason="test")
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_repair_succeeds_with_unitialized_bm(manager):
    await manager.server_add()
    await manager.server_add()
    servers = await manager.running_servers()

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH tombstone_gc = {'mode': 'repair'}")

    await manager.api.enable_injection(servers[1].ip_addr, "repair_flush_hints_batchlog_handler_bm_uninitialized", True, {})

    await manager.api.repair(servers[0].ip_addr, "ks", "tbl")

async def do_batchlog_flush_in_repair(manager, cache_time_in_ms):
    """
    Check that repair batchlog flush handler caches the flush request
    """
    nr_repairs_per_node = 3
    nr_repairs = 2 * nr_repairs_per_node
    total_repair_duration = 0

    cmdline = ["--repair-hints-batchlog-flush-cache-time-in-ms", str(cache_time_in_ms), "--smp", "1", "--logger-log-level", "api=trace"]
    node1 = await manager.server_add(cmdline=cmdline)
    node2 = await manager.server_add(cmdline=cmdline)

    cql = manager.get_cql()
    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY) WITH tombstone_gc = {'mode': 'repair'}")

    for node in (node1, node2):
        await manager.api.enable_injection(node.ip_addr, "repair_flush_hints_batchlog_handler", one_shot=False)
        await manager.api.enable_injection(node.ip_addr, "add_delay_to_batch_replay", one_shot=False)

    for node in (node1, node2):
        assert (await get_injection_params(manager, node.ip_addr, "repair_flush_hints_batchlog_handler")) == {}

    async def do_repair(node):
        await manager.api.repair(node.ip_addr, "ks", "tbl")

    async def repair(label):
        start = time.time()
        await asyncio.gather(*(do_repair(node) for x in range(nr_repairs_per_node) for node in [node1, node2]))
        duration = time.time() - start
        params = await get_injection_params(manager, node1.ip_addr, "repair_flush_hints_batchlog_handler")
        logger.debug(f"After {label} repair cache_time_in_ms={cache_time_in_ms} injection_params={params} repair_duration={duration}")
        return (params, duration)

    params, duration = await repair("First")
    total_repair_duration += duration

    await asyncio.sleep(1 + (cache_time_in_ms / 1000))

    params, duration = await repair("Second")
    total_repair_duration += duration

    assert (int(params['issue_flush']) > 0)
    if cache_time_in_ms > 0:
        assert (int(params['skip_flush']) > 0)
    else:
        assert (not 'skip_flush' in params)

    logger.debug(f"Repair nr_repairs={nr_repairs} cache_time_in_ms={cache_time_in_ms} total_repair_duration={total_repair_duration}")

@pytest.mark.skip(reason="test")
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_batchlog_flush_in_repair_with_cache(manager):
    await do_batchlog_flush_in_repair(manager, 5000);

@pytest.mark.skip(reason="test")
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_batchlog_flush_in_repair_without_cache(manager):
    await do_batchlog_flush_in_repair(manager, 0);

def add_net_delay(delay_in_ms=50):
    if delay_in_ms == 0:
        logger.info(f"Skipped adding net delay {delay_in_ms=}")
        return
    logger.info(f"Adding net delay {delay_in_ms=}")
    ret = os.system("sudo modprobe sch_netem");
    assert ret == 0
    ret = os.system("sudo tc qdisc del dev lo root")
    ret = os.system(f"sudo tc qdisc add dev lo root handle 1:0 netem delay {delay_in_ms}msec");
    assert ret == 0

def del_net_delay():
    logger.info(f"Removing net delay")
    ret = os.system("sudo tc qdisc del dev lo root")

async def do_repair_high_rf_with_gen_data(manager, enable_opt):
    net_delay = 0
    net_delay = 200
    net_delay = 50
    net_delay = 0
    net_delay = 30
    rf = 4
    rf = 3
    keyspace = 'test'
    table = 'test'
    key_nr = 10000000
    key_nr = 5000000
    key_nr = 1000000 # 16 Get rows call
    start_key = 1
    end_key = key_nr
    column_size = 34
    drop_ratio = 0.05
    enable_tablets = 'false'

    del_net_delay()

    cmdline = ["--hinted-handoff-enabled", "0", "--smp", "1", "--num-tokens", "1"]
    if enable_opt:
        cmdline += ["--enable-multiple-dc-opt", "1"]
    else:
        cmdline += ["--enable-multiple-dc-opt", "0"]

    for i in range(rf):
        await manager.server_add(cmdline=cmdline)
    servers = await manager.running_servers()

    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': {}}} AND tablets = {{'enabled': {}}};".format(rf, enable_tablets))
    await cql.run_async("CREATE TABLE test.test (pk blob PRIMARY KEY, c0 blob, c1 blob, c2 blob) WITH tombstone_gc = {'mode':'repair'};")


    async def insert_data(server):
        await manager.api.generate_data(server.ip_addr, keyspace, table, start_key, end_key, column_size, drop_ratio)

    await asyncio.gather(*[insert_data(server) for server in servers])

    try:
        if net_delay > 0:
            add_net_delay(net_delay)
        t1 = time.time()
        await manager.api.repair(servers[0].ip_addr, keyspace, table)
        t2 = time.time()
        duration = t2 - t1;
        logger.info(f"repair nodes={len(servers)} {duration=}s {key_nr=} {rf=} {net_delay=}ms {enable_opt=}")
    finally:
        del_net_delay()

async def do_repair_high_rf(manager, enable_opt):
    net_delay = 200
    net_delay = 100
    rf = 3 # 3+3 * 1M = 6M
    total_run = 3
    keys = 1000000

    rf = 9 # 3+9 * 0.5M = 6M
    total_run = 3
    keys = 500000

    rf = 6 # 3+6 * 0.5M = 4.5M
    total_run = 3
    keys = 100000


    rf = 3 # Median
    total_run = 3
    keys = 200000

    rf = 3 # Median works
    total_run = 3
    keys = 100000

    rf = 4 # Median works
    total_run = 3
    keys = 100000

    rf = 3 # Light
    total_run = 1
    keys = 1000

    rf = 6 # Median works
    total_run = 3
    keys = 100000

    del_net_delay()

    cmdline = ["--hinted-handoff-enabled", "0", "--smp", "1", "--num-tokens", "1"]
    if enable_opt:
        cmdline += ["--enable-multiple-dc-opt", "1"]
    else:
        cmdline += ["--enable-multiple-dc-opt", "0"]

    for i in range(rf):
        await manager.server_add(cmdline=cmdline)
    servers = await manager.running_servers()

    cql = manager.get_cql()

    try:
        res = list(cql.execute(f"SELECT count(*) FROM keyspace1.standard1"))
        logger.info(f"keyspace1 has got {res}")
    except:
        logger.info("keyspace1 does not exist")


    async def insert(ip, key_start, key_end):
        num = key_end - key_start
        cmd=f"cassandra-stress write no-warmup cl=QUORUM n={num} -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor={rf})' -mode cql3 native -rate 'threads=100 fixed=20000/s'  -col 'size=FIXED(128) n=FIXED(8)' -pop seq={key_start}..{key_end} -node {ip}"
        logger.info(f"Run {cmd=}")
        os.system(cmd)

    async def stop_and_insert(node, ip, key_start, key_end):
        await manager.server_stop_gracefully(node.server_id)
        await insert(ip, key_start, key_end)
        await manager.server_start(node.server_id)

    async def no_stop_insert(run):
        ip = servers[0].ip_addr
        s = run * keys
        logger.info(f"Insert on {run=} out of {total_run} start={s} end={s+keys}")
        await insert(ip, s, s + keys)

    # insert when no node is down
    await asyncio.gather(*[no_stop_insert(run) for run in range(total_run)])

    # insert when one node is down
    run = total_run
    for node in servers:
        s = run * keys
        run = run + 1
        logger.info(f"Stop and insert on {node.ip_addr} start={s} end={s + keys}")
        ip = servers[0].ip_addr if node.ip_addr != servers[0].ip_addr else servers[1].ip_addr
        await stop_and_insert(node, ip, s, s + keys)

    try:
        add_net_delay(net_delay)
        t1 = time.time()
        await manager.api.repair(servers[0].ip_addr, "keyspace1", "standard1")
        t2 = time.time()
        duration = t2 - t1;
        logger.info(f"repair nodes={len(servers)} {duration=}s {rf=} {enable_opt=}")
    finally:
        del_net_delay()

# async def test_repair_high_rf_without_opt(manager):
#     await do_repair_high_rf_with_gen_data(manager, False)

async def test_repair_high_rf_with_opt(manager):
    await do_repair_high_rf_with_gen_data(manager, True)
