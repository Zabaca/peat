#!/bin/bash

# like restore_from_wal.sh, but takes existing initdb.tar.zst

set -euxo pipefail

PG_BIN=$1
WAL_PATH=$2
DATA_DIR=$3
PORT=$4
PG_VERSION=${5:-18}
echo "port=$PORT" >> "$DATA_DIR"/postgresql.conf
echo "shared_preload_libraries='\$libdir/neon_rmgr.so'" >> "$DATA_DIR"/postgresql.conf
REDO_POS=0x$("$PG_BIN"/pg_controldata -D "$DATA_DIR" | grep -F "REDO location"| cut -c 42-)
# The size of the shutdown checkpoint record initdb writes: Postgres 19's checkpoint is 8 bytes
# larger (it records whether logical decoding is enabled).
case "$PG_VERSION" in
    19) CHECKPOINT_RECORD_SIZE=122 ;;
    *) CHECKPOINT_RECORD_SIZE=114 ;;
esac
# The checkpoint record's end as an offset in the first WAL segment (REDO_POS is a position in the
# WAL: past the first segment's 16 MB, it is not an offset into the file). What is copied from initdb's
# WAL is only that much of its first segment: Postgres 19 writes more records to it when the cluster is
# started and stopped below, and those must not replace the WAL that follows.
declare -i WAL_SIZE=($REDO_POS%16777216)+$CHECKPOINT_RECORD_SIZE
"$PG_BIN"/pg_ctl -D "$DATA_DIR" -l "$DATA_DIR/logfile.log" start
"$PG_BIN"/pg_ctl -D "$DATA_DIR" -l "$DATA_DIR/logfile.log" stop -m immediate
cp "$DATA_DIR"/pg_wal/000000010000000000000001 "$DATA_DIR"
cp "$WAL_PATH"/* "$DATA_DIR"/pg_wal/
for partial in "$DATA_DIR"/pg_wal/*.partial ; do mv "$partial" "${partial%.partial}" ; done
dd if="$DATA_DIR"/000000010000000000000001 of="$DATA_DIR"/pg_wal/000000010000000000000001 bs=$WAL_SIZE count=1 conv=notrunc
rm -f "$DATA_DIR"/000000010000000000000001
