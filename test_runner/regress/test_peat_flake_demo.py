"""TEMPORARY: the test as it was (one zone), run beside the changed one."""
from __future__ import annotations

import pytest
from fixtures.common_types import TimelineId
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.pg_version import PgVersion


def test_baseline_re_attach(neon_env_builder: NeonEnvBuilder):
    """
    This test assumes that the secondary location cannot be configured for whatever reason.
    It then attempts to detach and and attach the tenant back again and, finally, checks
    for observed state consistency by attempting to create a timeline.

    See LKB-204 for more details.
    """

    neon_env_builder.num_pageservers = 2

    env = neon_env_builder.init_configs()
    env.start()

    env.storage_controller.allowed_errors.append(".*failpoint.*")

    tenant_id, _ = env.create_tenant(shard_count=1, placement_policy='{"Attached":1}')
    env.storage_controller.reconcile_until_idle()

    locations = env.storage_controller.locate(tenant_id)
    assert len(locations) == 1
    primary: int = locations[0]["node_id"]

    not_primary = [ps.id for ps in env.pageservers if ps.id != primary]
    assert len(not_primary) == 1
    secondary = not_primary[0]

    env.get_pageserver(secondary).http_client().configure_failpoints(
        ("put-location-conf-handler", "return(1)")
    )

    env.storage_controller.tenant_policy_update(tenant_id, {"placement": "Detached"})

    with pytest.raises(Exception, match="failpoint"):
        env.storage_controller.reconcile_all()

    env.storage_controller.tenant_policy_update(tenant_id, {"placement": {"Attached": 1}})

    with pytest.raises(Exception, match="failpoint"):
        env.storage_controller.reconcile_all()

    env.storage_controller.pageserver_api().timeline_create(
        pg_version=PgVersion.NOT_SET, tenant_id=tenant_id, new_timeline_id=TimelineId.generate()
    )
