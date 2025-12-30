#!/bin/bash
# Random Read Benchmark Script for Curvine NFS Gateway
# Tests random read performance with various configurations

set -e

MOUNT_POINT="/mnt/curvine-nfs"
TEST_FILE="${MOUNT_POINT}/fio_random_read_source"
TEST_SIZE="1G"

echo "=========================================="
echo "Random Read Benchmark"
echo "=========================================="
echo "Mount Point: ${MOUNT_POINT}"
echo "Test File: ${TEST_FILE}"
echo "Test Size: ${TEST_SIZE}"
echo ""

# Check if mount point exists
if [ ! -d "${MOUNT_POINT}" ]; then
    echo "Error: Mount point ${MOUNT_POINT} does not exist"
    exit 1
fi

# Create test file if it doesn't exist or is too small
if [ ! -f "${TEST_FILE}" ] || [ $(stat -c%s "${TEST_FILE}") -lt 1073741824 ]; then
    echo "Creating test file (${TEST_SIZE})..."
    dd if=/dev/zero of="${TEST_FILE}" bs=1M count=1024 status=progress
    echo ""
fi

# Clear page cache
echo "Clearing page cache..."
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo ""

# Test 1: Random read with 4K blocks (typical database workload)
echo "Test 1: Random read (4K blocks, iodepth=1, single thread)"
echo "------------------------------------------------------"
fio --name=rand-read-4k \
    --filename="${TEST_FILE}" \
    --bs=4K \
    --rw=randread \
    --direct=1 \
    --ioengine=psync \
    --iodepth=1 \
    --numjobs=1 \
    --runtime=30 \
    --time_based \
    --group_reporting

echo ""
echo "Test 2: Random read (64K blocks, iodepth=1, single thread)"
echo "------------------------------------------------------"
fio --name=rand-read-64k \
    --filename="${TEST_FILE}" \
    --bs=64K \
    --rw=randread \
    --direct=1 \
    --ioengine=psync \
    --iodepth=1 \
    --numjobs=1 \
    --runtime=30 \
    --time_based \
    --group_reporting

echo ""
echo "Test 3: Random read (256K blocks, iodepth=1, single thread)"
echo "------------------------------------------------------"
fio --name=rand-read-256k \
    --filename="${TEST_FILE}" \
    --bs=256K \
    --rw=randread \
    --direct=1 \
    --ioengine=psync \
    --iodepth=1 \
    --numjobs=1 \
    --runtime=30 \
    --time_based \
    --group_reporting

echo ""
echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
