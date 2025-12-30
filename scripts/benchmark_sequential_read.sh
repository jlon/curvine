#!/bin/bash
# Sequential Read Benchmark Script
# 用于建立统一的测试基准

set -e

MOUNT_POINT="/mnt/curvine-nfs"
TEST_FILE="$MOUNT_POINT/fio_read_source"
TEST_SIZE="100M"

echo "=========================================="
echo "Sequential Read Benchmark"
echo "=========================================="
echo "Mount Point: $MOUNT_POINT"
echo "Test File: $TEST_FILE"
echo "Test Size: $TEST_SIZE"
echo ""

# Ensure test file exists
if [ ! -f "$TEST_FILE" ]; then
    echo "Creating test file..."
    dd if=/dev/zero of="$TEST_FILE" bs=1M count=100 status=progress
    echo ""
fi

# Clear page cache
echo "Clearing page cache..."
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo ""

# Run sequential read test
echo "Running sequential read test (direct I/O, 1M block size)..."
echo ""

fio --name=seq-read \
    --filename="$TEST_FILE" \
    --bs=1M \
    --rw=read \
    --direct=1 \
    --ioengine=psync \
    --iodepth=1 \
    --runtime=30 \
    --time_based \
    --group_reporting

echo ""
echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
