from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from fixtures.common_types import TenantShardId, TimelineId
from fixtures.pageserver.http import PageserverApiException
from fixtures.pg_version import PgVersion
from fixtures.utils import run_only_on_default_postgres

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


#
# Peat builds Postgres 18 only, and its storage image carries only 18's initdb, so a
# timeline created without a Postgres version must be 18 (Neon's default is 17, which a
# Peat image cannot create). Both through the pageserver and through the storage
# controller.
#
@run_only_on_default_postgres("the default is the same whatever the run's version")
def test_timeline_without_pg_version_is_18(neon_simple_env: NeonEnv):
    env = neon_simple_env
    tenant_id = env.initial_tenant
    ps_http = env.pageserver.http_client()

    direct = TimelineId.generate()
    ps_http.timeline_create(PgVersion.NOT_SET, tenant_id, direct)
    assert ps_http.timeline_detail(tenant_id, direct)["pg_version"] == 18

    via_controller = TimelineId.generate()
    env.storage_controller.pageserver_api().timeline_create(
        PgVersion.NOT_SET, tenant_id, via_controller
    )
    assert ps_http.timeline_detail(tenant_id, via_controller)["pg_version"] == 18


#
# A bootstrap from another timeline's initdb (existing_initdb_timeline_id) must say that
# initdb's version: the default would label that initdb's data with the default version,
# whatever version made it. The pageserver refuses it without one; the storage controller,
# which uses this to create a sharded timeline's other shards from shard 0's initdb, passes
# shard 0's version on.
#
@run_only_on_default_postgres("the default is the same whatever the run's version")
def test_existing_initdb_needs_pg_version(neon_simple_env: NeonEnv):
    env = neon_simple_env
    tenant_id = env.initial_tenant
    ps_http = env.pageserver.http_client()

    source = TimelineId.generate()
    ps_http.timeline_create(env.pg_version, tenant_id, source)
    with pytest.raises(PageserverApiException, match="pg_version is required") as e:
        ps_http.timeline_create(
            PgVersion.NOT_SET,
            tenant_id,
            TimelineId.generate(),
            existing_initdb_timeline_id=source,
        )
    assert e.value.status_code == 400

    # Through the storage controller, a sharded tenant's timeline created with no version:
    # every shard's timeline is 18.
    sharded, _ = env.create_tenant(shard_count=2)
    timeline_id = TimelineId.generate()
    env.storage_controller.pageserver_api().timeline_create(PgVersion.NOT_SET, sharded, timeline_id)
    shards = env.storage_controller.locate(sharded)
    assert len(shards) == 2
    for shard in shards:
        detail = ps_http.timeline_detail(TenantShardId.parse(shard["shard_id"]), timeline_id)
        assert detail["pg_version"] == 18
