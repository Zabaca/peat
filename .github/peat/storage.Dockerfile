# Peat's storage image: the four storage programs (storage broker, storage controller,
# safekeeper, pageserver) and the storage controller's copy tool (storcon_records), built
# with the default features (no test-only HTTP API), and
# the part of Postgres 18 and of Postgres 19 the pageserver runs itself (initdb for new
# timelines, postgres for WAL redo, with their libraries and data files) at /usr/local/v18
# and /usr/local/v19, where pg_distrib_dir '/usr/local/' finds them, as in Neon's own image. Also sh and curl, which the lab's scripts
# use inside the storage pods. On the Ubuntu the hosted runner built them on, pinned by
# digest. The build context is the unpacked peat-images artifact (package-images.sh).
FROM ubuntu:24.04@sha256:73b999393d07f2bf65a40373778a44dc9a6d54ad7718d29247d8ff9413c4d6a8
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/Zabaca/peat" \
      org.opencontainers.image.title="Peat storage" \
      org.opencontainers.image.description="Peat's storage broker, storage controller, safekeeper and pageserver, with Postgres 18 and 19 for the pageserver. A fork of Neon's code; not affiliated with or endorsed by Neon or Databricks." \
      org.opencontainers.image.licenses="Apache-2.0 AND PostgreSQL" \
      org.opencontainers.image.revision="${REVISION}"
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates curl libcurl4t64 libicu74 libldap2 libgssapi-krb5-2 liblz4-1 \
      libreadline8t64 libseccomp2 libssl3t64 libxml2 libzstd1 zlib1g \
 && rm -rf /var/lib/apt/lists/* \
 && userdel --remove ubuntu \
 && useradd --uid 1000 --home-dir /data --create-home peat
COPY bin/storage_broker bin/storage_controller bin/storcon_records bin/safekeeper bin/pageserver /usr/local/bin/
COPY pg/v18/bin/initdb pg/v18/bin/postgres /usr/local/v18/bin/
COPY pg/v18/lib /usr/local/v18/lib
COPY pg/v18/share /usr/local/v18/share
COPY pg/v19/bin/initdb pg/v19/bin/postgres /usr/local/v19/bin/
COPY pg/v19/lib /usr/local/v19/lib
COPY pg/v19/share /usr/local/v19/share
COPY LICENSE NOTICE PROVENANCE /usr/share/doc/peat/
# Fail the build if any program or library is missing a shared library. The pageserver runs
# initdb and postgres with LD_LIBRARY_PATH set to the version's lib directory, so the check
# does too.
RUN ! (LD_LIBRARY_PATH=/usr/local/v18/lib ldd /usr/local/bin/* /usr/local/v18/bin/* \
         /usr/local/v18/lib/*.so* /usr/local/v18/lib/postgresql/*.so | grep 'not found') \
 && ! (LD_LIBRARY_PATH=/usr/local/v19/lib ldd /usr/local/v19/bin/* \
         /usr/local/v19/lib/*.so* /usr/local/v19/lib/postgresql/*.so | grep 'not found')
USER peat
WORKDIR /data
