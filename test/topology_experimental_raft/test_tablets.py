#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts

import pytest
import asyncio
import logging
import time


logger = logging.getLogger(__name__)


async def inject_error_one_shot_on(manager, error_name, servers):
    errs = [inject_error_one_shot(manager.api, s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)


@pytest.mark.asyncio
async def test_bootstrap(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    #servers = [await manager.server_add(), await manager.server_add(), await manager.server_add()]
    servers = [await manager.server_add()]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': 1, 'initial_tablets': 32};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    # for s in servers:
    #     await manager.server_restart(s.server_id)

    async def check():
        logger.info("Checking table")
        rows = await cql.run_async("SELECT * FROM test.test;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await inject_error_on(manager, "tablet_allocator_shuffle", servers)

    logger.info("Adding new server 3")
    await manager.server_add()

    #logger.info("Restart before check")
    # for s in servers:
    #     await manager.server_restart(s.server_id)

    await check()
    time.sleep(5) # Give load balancer some time to do work
    await check()

    if True:
        logger.info("Adding new server 4")
        await manager.server_add()

        await check()
        time.sleep(5) # Give load balancer some time to do work
        await check()

    await cql.run_async("DROP KEYSPACE test;")
