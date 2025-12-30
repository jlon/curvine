#!/bin/bash
# Multi-threaded Random Read Benchmark
# Tests if IO depth is the bottleneck

set -e

MOUNT_POINT="/mnt/curvine-nfs"
TEST_FILE="${MOUNT_POINT}/fio_random_read_source"

echo "=========================================="
echo "Multi-threaded Random Read Benchmark"
echo "=========================================="
echo "Mount Point: ${MOUNT_POINT}"
echo "Test File: ${TEST_FILE}"
echo ""

# Check if test file exists
if [ ! -f "${TEST_FILE}" ]; then
    echo "Error: Test file ${TEST_FILE} does not exist"
    echo "Please run benchmark_random_read.sh first to create the test file"
    exit 1
fi

# Clear page cache
echo "Clearing page cache..."
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo ""

# Test 1: 4K random read with 4 threads
echo "Test 1: 4K random read (4 threads, iodepth=1 each)"
echo "------------------------------------------------------"
fio --name=rand-read-4k-4t \
    --filename="${TEST_FILE}" \
    --bs=4K \
    --rw=randread \
    --direct=1 \
    --ioengine=psync \
    --iodepth=1 \
    --numjobs=4 \
    --runtime=30 \
    --time_based \
    --group_reporting

echo ""
echo "Test 2: 4K random read (8 threads, iodepth=1 each)"
echo "------------------------------------------------------"
fio --name=rand-read-4k-8t \
    --filename="${TEST_FILE}" \
    --bs=4K \
    --rw=randread \
    --direct=1 \
    --ioengine=psync \
    --iodepth=1 \
    --numjobs=8 \
    --runtime=30 \
    --time_based \
    --group_reporting

echo ""
echo "Test 3: 64K random read (4 threads, iodepth=1 each)"
echo "------------------------------------------------------"
fio --name=rand-read-64k-4t \
    --filename="${TEST_FILE}" \
    --bs=64K \
    --rw=randread \
    --direct=1 \
    --ioengine=psync \
    --iodepth=1 \
    --numjobs=4 \
    --runtime=30 \
    --time_based \
    --group_reporting

echo ""
echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
