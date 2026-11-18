# Peat's compute image for Postgres 18: Postgres 18 with Neon's extension (neon, neon_rmgr,
# neon_utils, neon_walredo) installed at /usr/local, and Neon's compute manager compute_ctl
# as the entrypoint, built with the default features. Only what the lab's control layer
# and tests use: compute_ctl starts and configures Postgres from a spec, and the lab's
# readiness probe runs psql. Left out of Neon's compute image: the third-party extensions
# (PostGIS, pgvector and dozens more), pgbouncer, local_proxy, fast_import, the metrics
# exporters, rsyslog and debugging tools. On the Ubuntu the hosted runner built them on,
# pinned by digest. The build context is the unpacked peat-images artifact.
FROM ubuntu:24.04@sha256:73b999393d07f2bf65a40373778a44dc9a6d54ad7718d29247d8ff9413c4d6a8
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/Zabaca/peat" \
      org.opencontainers.image.title="Peat compute, Postgres 18" \
      org.opencontainers.image.description="Postgres 18 with Neon's extension and compute_ctl, from Peat. A fork of Neon's code; not affiliated with or endorsed by Neon or Databricks." \
      org.opencontainers.image.licenses="Apache-2.0 AND PostgreSQL" \
      org.opencontainers.image.revision="${REVISION}"
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates libcurl4t64 libicu74 libldap2 libgssapi-krb5-2 liblz4-1 \
      libreadline8t64 libseccomp2 libssl3t64 libxml2 libzstd1 zlib1g \
 && rm -rf /var/lib/apt/lists/* \
 && userdel --remove ubuntu \
 && useradd --uid 1000 --create-home --home-dir /var/db/postgres postgres \
 && mkdir -p -m 0750 /var/db/postgres/compute \
 && mkdir -p /var/db/postgres/specs /usr/local/download_extensions \
 && chown -R postgres:postgres /var/db/postgres /usr/local/download_extensions \
 && mkdir -p -m 777 /neon/cache
COPY pg/v18/bin /usr/local/bin
COPY pg/v18/lib /usr/local/lib
COPY pg/v18/share /usr/local/share
COPY bin/compute_ctl /usr/local/bin/compute_ctl
COPY LICENSE NOTICE PROVENANCE /usr/share/doc/peat/
RUN ldconfig \
 && ! (ldd /usr/local/bin/* /usr/local/lib/*.so* /usr/local/lib/postgresql/*.so | grep 'not found')
USER postgres
ENTRYPOINT ["/usr/local/bin/compute_ctl"]
