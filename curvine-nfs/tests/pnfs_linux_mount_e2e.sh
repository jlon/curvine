#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.."; pwd)"
DIST_DIR="$ROOT_DIR/build/dist"
DIST_BIN="$DIST_DIR/bin"
DIST_LIB="$DIST_DIR/lib"
DIST_CONF="$DIST_DIR/conf/curvine-cluster.toml"
TMP_CONF="/tmp/curvine-pnfs-linux-e2e.toml"
MOUNT_POINT="/mnt/curvine-pnfs-e2e"
NFS_SECRET="pnfs-linux-e2e-secret"
MASTER_RPC_PORT="${MASTER_RPC_PORT:-18995}"
JOURNAL_RPC_PORT="${JOURNAL_RPC_PORT:-18996}"
WORKER_RPC_PORT="${WORKER_RPC_PORT:-18997}"
MASTER_WEB_PORT="${MASTER_WEB_PORT:-19000}"
WORKER_WEB_PORT="${WORKER_WEB_PORT:-19001}"
MDS_NFS_PORT="${MDS_NFS_PORT:-2049}"
WORKER_NFS_PORT="${WORKER_NFS_PORT:-2050}"
TEST_FILE="$MOUNT_POINT/pnfs-e2e.bin"
SOURCE_FILE="/tmp/pnfs-e2e-source.bin"
FILE_MB="${FILE_MB:-8}"
WORKER_READ_LOG_START=0

cleanup() {
    set +e
    if mountpoint -q "$MOUNT_POINT"; then
        sudo umount -lf "$MOUNT_POINT"
    fi
    "$DIST_BIN/curvine-nfs-gateway.sh" stop >/dev/null 2>&1 || true
    "$DIST_BIN/curvine-worker.sh" stop >/dev/null 2>&1 || true
    "$DIST_BIN/curvine-master.sh" stop >/dev/null 2>&1 || true
    rm -f "$TMP_CONF"
    rm -f "$SOURCE_FILE"
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "missing required command: $1" >&2
        exit 1
    }
}

prepare_conf() {
    cp "$DIST_CONF" "$TMP_CONF"

    perl -0pi -e \
        's/\[master\]\nmeta_dir = "testing\/meta"/[master]\nrpc_port = '"$MASTER_RPC_PORT"'\nweb_port = '"$MASTER_WEB_PORT"'\nmeta_dir = "testing\/meta"/g' \
        "$TMP_CONF"

    perl -0pi -e \
        's/\[journal\]\njournal_addrs = \[\n    \{id = 1, hostname = "localhost", port = 8996\}\n\]/[journal]\nrpc_port = '"$JOURNAL_RPC_PORT"'\njournal_addrs = \[\n    {id = 1, hostname = "localhost", port = '"$JOURNAL_RPC_PORT"'}\n\]/g' \
        "$TMP_CONF"

    perl -0pi -e \
        's/\[worker\]\ndir_reserved = "0"/[worker]\nrpc_port = '"$WORKER_RPC_PORT"'\nweb_port = '"$WORKER_WEB_PORT"'\ndir_reserved = "0"/g' \
        "$TMP_CONF"

    perl -0pi -e \
        's/enable_s3_gateway = false/enable_s3_gateway = false\nenable_nfs_gateway = true\nnfs_gateway_port = '"$WORKER_NFS_PORT"'/g' \
        "$TMP_CONF"

    perl -0pi -e \
        's/\[client\]\n/\[client\]\nblock_size = "1MB"\n/g' \
        "$TMP_CONF"

    cat >>"$TMP_CONF" <<EOF

[nfs_gateway]
pnfs_ds_secret = "$NFS_SECRET"
EOF
}

build_and_deploy() {
    echo "Building curvine-server and curvine-nfs..."
    cargo build --release -p curvine-server -p curvine-nfs 2>&1 | tail -20

    cp "$ROOT_DIR/target/release/curvine-server" "$DIST_LIB/"
    cp "$ROOT_DIR/target/release/curvine-nfs-gateway" "$DIST_LIB/"

    if [ ! -f "$DIST_BIN/curvine-nfs-gateway.sh" ]; then
        cp "$ROOT_DIR/build/bin/curvine-nfs-gateway.sh" "$DIST_BIN/"
        chmod +x "$DIST_BIN/curvine-nfs-gateway.sh"
    fi
}

reset_logs() {
    mkdir -p "$DIST_DIR/logs"
    : > "$DIST_DIR/logs/master.out"
    : > "$DIST_DIR/logs/worker.out"
    : > "$DIST_DIR/logs/curvine-nfs-gateway.out"
}

start_services() {
    echo "Starting Curvine master and worker with dist scripts..."
    CURVINE_CONF_FILE="$TMP_CONF" "$DIST_BIN/curvine-master.sh" start
    CURVINE_CONF_FILE="$TMP_CONF" "$DIST_BIN/curvine-worker.sh" start
    sleep 8

    echo "Starting curvine-nfs-gateway..."
    CURVINE_CONF_FILE="$TMP_CONF" "$DIST_BIN/curvine-nfs-gateway.sh" start --conf "$TMP_CONF" --listen "0.0.0.0:${MDS_NFS_PORT}"
    sleep 5
}

wait_for_worker_registration() {
    local master_log="$DIST_DIR/logs/master.out"
    local worker_log="$DIST_DIR/logs/worker.out"
    local timeout=30
    local elapsed=0

    while [ "$elapsed" -lt "$timeout" ]; do
        if grep -q "Worker register:" "$master_log" && grep -q "worker register success" "$worker_log"; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    echo "worker registration did not complete within ${timeout}s" >&2
    exit 1
}

mount_nfs() {
    sudo mkdir -p "$MOUNT_POINT"
    if mountpoint -q "$MOUNT_POINT"; then
        sudo umount -lf "$MOUNT_POINT"
    fi

    sudo mount -t nfs -o vers=4.1,port="${MDS_NFS_PORT}",tcp,resvport 127.0.0.1:/ "$MOUNT_POINT"
}

prepare_data() {
    truncate -s "${FILE_MB}M" "$SOURCE_FILE"
    "$DIST_BIN/cv" -c "$TMP_CONF" fs rm /pnfs-e2e.bin >/dev/null 2>&1 || true
    "$DIST_BIN/cv" -c "$TMP_CONF" fs put "$SOURCE_FILE" /pnfs-e2e.bin
}

read_via_nfs() {
    sudo dd if="$TEST_FILE" of=/dev/null bs=1M status=none
}

verify_ds_read() {
    local worker_log="$DIST_DIR/logs/worker.out"
    local start_line=$((WORKER_READ_LOG_START + 1))

    if ! grep -q "pNFS DS read-only mode enabled" "$worker_log"; then
        echo "worker-side pNFS DS mode was not enabled" >&2
        exit 1
    fi

    if ! tail -n +"$start_line" "$worker_log" | grep -q "pNFS DS READ:"; then
        echo "did not observe pNFS DS READ in worker log" >&2
        exit 1
    fi
}

main() {
    trap cleanup EXIT

    require_cmd cargo
    require_cmd perl
    require_cmd mountpoint
    require_cmd sudo

    prepare_conf
    build_and_deploy
    reset_logs
    start_services
    wait_for_worker_registration
    prepare_data
    mount_nfs
    WORKER_READ_LOG_START=$(wc -l < "$DIST_DIR/logs/worker.out" 2>/dev/null || echo 0)
    read_via_nfs
    verify_ds_read

    echo "pNFS Linux mount e2e passed"
}

main "$@"
