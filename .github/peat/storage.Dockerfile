# Peat's slim storage image: only the four storage programs from the release build
# (storage broker, storage controller, safekeeper, pageserver), on the same Ubuntu the
# hosted runner built them on. The build context is the unpacked CI artifact.
# Before the first publish, pin the base image by digest.
FROM ubuntu:24.04
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates libpq5 libssl3t64 \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 1000 --home-dir /data --create-home peat
COPY target/release/storage_broker target/release/storage_controller \
     target/release/safekeeper target/release/pageserver /usr/local/bin/
COPY LICENSE NOTICE /usr/share/doc/peat/
USER peat
WORKDIR /data
