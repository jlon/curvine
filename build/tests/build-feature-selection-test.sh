#!/usr/bin/env bash
#
# Verify build/build.sh preserves explicit UFS selections for exported
# client-side artifacts while keeping server-native features isolated.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_SH="$ROOT/build/build.sh"

require_literal_in_block() {
  local block_start="$1"
  local block_end="$2"
  local needle="$3"
  local message="$4"

  if ! awk -v block_start="$block_start" -v block_end="$block_end" -v needle="$needle" "
    index(\$0, block_start) { in_block = 1 }
    in_block && index(\$0, needle) { found = 1 }
    in_block && index(\$0, block_end) { exit found ? 0 : 1 }
    END { if (!in_block) exit 1 }
  " "$BUILD_SH"; then
    echo "FAIL: ${message}" >&2
    exit 1
  fi
}

require_literal_in_block \
  'if [ ${#CLIENT_RUST_BUILD_ARGS[@]} -gt 0 ]; then' \
  'CLIENT_FEATURES=' \
  'append_ufs_features "client-safe"' \
  'client/fuse build must include explicit --ufs features'

require_literal_in_block \
  'CLIENT_FEATURES=' \
  'CLI_FEATURES=' \
  'append_ufs_features "cli-minimal"' \
  'cli build must include explicit --ufs features'

if rg -n 'is_client_native_ufs|remember_skipped_native_ufs|CLIENT_SKIPPED_NATIVE_UFS|Client-safe/minimal' "$BUILD_SH" >/dev/null; then
  echo "FAIL: build script must not silently drop native UFS features from requested client artifacts" >&2
  exit 1
fi

if ! rg -F 'oss-hdfs|opendal-s3|opendal-oss|opendal-gcs|opendal-azblob|opendal-cos|opendal-hdfs|opendal-webhdfs|opendal-hdfs-native)' "$BUILD_SH" >/dev/null; then
  echo "FAIL: extra feature routing must cover all supported UFS selections" >&2
  exit 1
fi

if ! rg -F "server_native_pattern='libibverbs\\.so|librdmacm\\.so|libspdk|libdpdk|librte_'" "$BUILD_SH" >/dev/null; then
  echo "FAIL: client artifact dependency check must still forbid server-native RDMA/SPDK deps" >&2
  exit 1
fi

if ! rg -F "native_ufs_pattern='libjindosdk|libhdfs|libjvm|libjli'" "$BUILD_SH" >/dev/null; then
  echo "FAIL: client artifact dependency check must separately track native UFS deps" >&2
  exit 1
fi

echo "PASS: build feature selection preserves requested UFS features"
