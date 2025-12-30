#!/bin/bash
# Comprehensive NFS vs FUSE Write Performance Benchmark
# 完整的NFS vs FUSE写入性能对比测试

set -e

NFS_FILE="/mnt/curvine-nfs/testfile_write"
FUSE_FILE="/curvine-fuse/testfile_write"
RUNTIME=30

echo "========================================="
echo "NFS vs FUSE 写入性能对比测试"
echo "测试时间: $(date)"
echo "========================================="
echo ""

# Function to run fio write test
run_fio_write() {
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
        --size=1G \
        --runtime="$RUNTIME" \
        --time_based \
        --group_reporting \
        --output-format=normal 2>&1 | grep -E "(WRITE:|READ:|bw=)" | head -3
    
    echo ""
}

echo "========================================="
echo "1. 顺序写测试 (1M块大小)"
echo "========================================="
echo ""

echo "--- NFS Gateway ---"
run_fio_write "nfs-seq-write-st" "$NFS_FILE" "1M" "write" "psync" 1 1
run_fio_write "nfs-seq-write-4t" "$NFS_FILE" "1M" "write" "psync" 1 4
run_fio_write "nfs-seq-write-8t" "$NFS_FILE" "1M" "write" "psync" 1 8
run_fio_write "nfs-seq-write-aio16" "$NFS_FILE" "1M" "write" "libaio" 16 1
run_fio_write "nfs-seq-write-aio32" "$NFS_FILE" "1M" "write" "libaio" 32 1

echo "--- FUSE ---"
run_fio_write "fuse-seq-write-st" "$FUSE_FILE" "1M" "write" "psync" 1 1
run_fio_write "fuse-seq-write-4t" "$FUSE_FILE" "1M" "write" "psync" 1 4
run_fio_write "fuse-seq-write-8t" "$FUSE_FILE" "1M" "write" "psync" 1 8
run_fio_write "fuse-seq-write-aio16" "$FUSE_FILE" "1M" "write" "libaio" 16 1
run_fio_write "fuse-seq-write-aio32" "$FUSE_FILE" "1M" "write" "libaio" 32 1

echo "========================================="
echo "2. 随机写测试 (4K块大小)"
echo "========================================="
echo ""

echo "--- NFS Gateway ---"
run_fio_write "nfs-rand-write-4k" "$NFS_FILE" "4K" "randwrite" "psync" 1 1

echo "--- FUSE ---"
run_fio_write "fuse-rand-write-4k" "$FUSE_FILE" "4K" "randwrite" "psync" 1 1

echo "========================================="
echo "3. 混合读写测试 (1M块大小, 70%读30%写)"
echo "========================================="
echo ""

echo "--- NFS Gateway ---"
run_fio_write "nfs-rw-mix-st" "$NFS_FILE" "1M" "rw" "psync" 1 1
run_fio_write "nfs-rw-mix-4t" "$NFS_FILE" "1M" "rw" "psync" 1 4

echo "--- FUSE ---"
run_fio_write "fuse-rw-mix-st" "$FUSE_FILE" "1M" "rw" "psync" 1 1
run_fio_write "fuse-rw-mix-4t" "$FUSE_FILE" "1M" "rw" "psync" 1 4

echo "========================================="
echo "测试完成: $(date)"
echo "========================================="

# Cleanup
echo ""
echo "清理测试文件..."
rm -f "$NFS_FILE" "$FUSE_FILE"
echo "完成！"
