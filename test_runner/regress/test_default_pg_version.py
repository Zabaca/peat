from __future__ import annotations

from typing import TYPE_CHECKING

from fixtures.common_types import TimelineId
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
