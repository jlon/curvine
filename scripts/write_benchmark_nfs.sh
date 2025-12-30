#!/bin/bash
# NFS Write Performance Benchmark

set -e

NFS_FILE="/mnt/curvine-nfs/testfile_write"
RUNTIME=30

echo "========================================="
echo "NFS Gateway 写入性能测试"
echo "测试时间: $(date)"
echo "========================================="
echo ""

run_fio() {
    local name=$1
    local bs=$2
    local rw=$3
    local engine=$4
    local iodepth=$5
    local numjobs=$6
    
    echo "测试: $name (bs=$bs, $rw, $engine, depth=$iodepth, jobs=$numjobs)"
    fio --name="$name" \
        --filename="$NFS_FILE" \
        --bs="$bs" \
        --rw="$rw" \
        --direct=1 \
        --ioengine="$engine" \
        --iodepth="$iodepth" \
        --numjobs="$numjobs" \
        --size=1G \
        --runtime="$RUNTIME" \
        --time_based \
        --group_reporting 2>&1 | grep -E "(READ:|WRITE:|bw=)" | head -3
    echo ""
}

echo "1. 顺序写 (1M)"
run_fio "seq-write-1t" "1M" "write" "psync" 1 1
run_fio "seq-write-4t" "1M" "write" "psync" 1 4
run_fio "seq-write-8t" "1M" "write" "psync" 1 8
run_fio "seq-write-aio16" "1M" "write" "libaio" 16 1
run_fio "seq-write-aio32" "1M" "write" "libaio" 32 1

echo "2. 随机写 (4K)"
run_fio "rand-write-4k" "4K" "randwrite" "psync" 1 1

echo "3. 混合读写 (1M, 70%读30%写)"
run_fio "rw-mix-1t" "1M" "rw" "psync" 1 1
run_fio "rw-mix-4t" "1M" "rw" "psync" 1 4

echo "完成！"
rm -f "$NFS_FILE"
