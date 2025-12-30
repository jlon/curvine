#!/bin/bash
# Comprehensive NFS vs FUSE Performance Benchmark
# 完整的NFS vs FUSE性能对比测试

set -e

NFS_FILE="/mnt/curvine-nfs/testfile"
FUSE_FILE="/curvine-fuse/testfile"
RUNTIME=30

echo "========================================="
echo "NFS vs FUSE 完整性能对比测试"
echo "测试时间: $(date)"
echo "========================================="
echo ""

# Function to run fio test
run_fio() {
    local name=$1
    local file=$2
    local bs=$3
    local rw=$4
    local engine=$5
    local iodepth=$6
    local numjobs=$7
    
    echo "测试: $name"
    echo "  文件: $file"
    echo "  参数: bs=$bs rw=$rw engine=$engine iodepth=$iodepth numjobs=$numjobs"
    
    fio --name="$name" \
        --filename="$file" \
        --bs="$bs" \
        --rw="$rw" \
        --direct=1 \
        --ioengine="$engine" \
        --iodepth="$iodepth" \
        --numjobs="$numjobs" \
        --runtime="$RUNTIME" \
        --time_based \
        --group_reporting \
        --output-format=normal 2>&1 | grep -E "(READ:|WRITE:|bw=)" | head -3
    
    echo ""
}

echo "========================================="
echo "1. 顺序读测试 (1M块大小)"
echo "========================================="
echo ""

echo "--- NFS Gateway ---"
run_fio "nfs-seq-read-st" "$NFS_FILE" "1M" "read" "psync" 1 1
run_fio "nfs-seq-read-4t" "$NFS_FILE" "1M" "read" "psync" 1 4
run_fio "nfs-seq-read-8t" "$NFS_FILE" "1M" "read" "psync" 1 8
run_fio "nfs-seq-read-aio16" "$NFS_FILE" "1M" "read" "libaio" 16 1
run_fio "nfs-seq-read-aio32" "$NFS_FILE" "1M" "read" "libaio" 32 1

echo "--- FUSE ---"
run_fio "fuse-seq-read-st" "$FUSE_FILE" "1M" "read" "psync" 1 1
run_fio "fuse-seq-read-4t" "$FUSE_FILE" "1M" "read" "psync" 1 4
run_fio "fuse-seq-read-8t" "$FUSE_FILE" "1M" "read" "psync" 1 8
run_fio "fuse-seq-read-aio16" "$FUSE_FILE" "1M" "read" "libaio" 16 1
run_fio "fuse-seq-read-aio32" "$FUSE_FILE" "1M" "read" "libaio" 32 1

echo "========================================="
echo "2. 随机读测试 (4K块大小)"
echo "========================================="
echo ""

echo "--- NFS Gateway ---"
run_fio "nfs-rand-read-4k" "$NFS_FILE" "4K" "randread" "psync" 1 1

echo "--- FUSE ---"
run_fio "fuse-rand-read-4k" "$FUSE_FILE" "4K" "randread" "psync" 1 1

echo "========================================="
echo "3. 顺序写测试 (1M块大小)"
echo "========================================="
echo ""

echo "--- NFS Gateway ---"
run_fio "nfs-seq-write-st" "$NFS_FILE" "1M" "write" "psync" 1 1

echo "--- FUSE ---"
run_fio "fuse-seq-write-st" "$FUSE_FILE" "1M" "write" "psync" 1 1

echo "========================================="
echo "测试完成: $(date)"
echo "========================================="
