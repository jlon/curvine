#!/bin/bash
# Quick benchmark: NFS Gateway vs FUSE (single run, key metrics)

set -e

NFS_MOUNT="/mnt/curvine-nfs"
FUSE_MOUNT="/curvine-fuse"

echo "=========================================="
echo "Quick Benchmark: NFS vs FUSE"
echo "=========================================="

# Prepare test files
NFS_FILE="${NFS_MOUNT}/fio_test_quick"
FUSE_FILE="${FUSE_MOUNT}/fio_test_quick"

if [ ! -f "${NFS_FILE}" ] || [ $(stat -c%s "${NFS_FILE}") -lt 1073741824 ]; then
    echo "Creating NFS test file..."
    dd if=/dev/zero of="${NFS_FILE}" bs=1M count=1024 status=none
fi

if [ ! -f "${FUSE_FILE}" ] || [ $(stat -c%s "${FUSE_FILE}") -lt 1073741824 ]; then
    echo "Creating FUSE test file..."
    dd if=/dev/zero of="${FUSE_FILE}" bs=1M count=1024 status=none
fi

echo ""
echo "Test 1: Sequential Read (1M, direct)"
echo "--------------------------------------"
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo "NFS:"
fio --name=nfs-seq-read --filename="${NFS_FILE}" --bs=1M --rw=read --direct=1 --ioengine=psync --iodepth=1 --runtime=30 --time_based | grep -E "READ:|bw="

sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo "FUSE:"
fio --name=fuse-seq-read --filename="${FUSE_FILE}" --bs=1M --rw=read --direct=1 --ioengine=psync --iodepth=1 --runtime=30 --time_based | grep -E "READ:|bw="

echo ""
echo "Test 2: Sequential Write (1M, direct)"
echo "--------------------------------------"
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo "NFS:"
fio --name=nfs-seq-write --filename="${NFS_MOUNT}/fio_write_quick" --size=100M --bs=1M --rw=write --direct=1 --ioengine=psync --iodepth=1 --runtime=30 --time_based | grep -E "WRITE:|bw="

sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo "FUSE:"
fio --name=fuse-seq-write --filename="${FUSE_MOUNT}/fio_write_quick" --size=100M --bs=1M --rw=write --direct=1 --ioengine=psync --iodepth=1 --runtime=30 --time_based | grep -E "WRITE:|bw="

echo ""
echo "Test 3: Random Read 4K (direct)"
echo "--------------------------------------"
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo "NFS:"
fio --name=nfs-rand-4k --filename="${NFS_FILE}" --bs=4K --rw=randread --direct=1 --ioengine=psync --iodepth=1 --runtime=30 --time_based | grep -E "READ:|bw=|IOPS="

sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo "FUSE:"
fio --name=fuse-rand-4k --filename="${FUSE_FILE}" --bs=4K --rw=randread --direct=1 --ioengine=psync --iodepth=1 --runtime=30 --time_based | grep -E "READ:|bw=|IOPS="

echo ""
echo "Test 4: Random Read 64K (direct)"
echo "--------------------------------------"
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo "NFS:"
fio --name=nfs-rand-64k --filename="${NFS_FILE}" --bs=64K --rw=randread --direct=1 --ioengine=psync --iodepth=1 --runtime=30 --time_based | grep -E "READ:|bw=|IOPS="

sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo "FUSE:"
fio --name=fuse-rand-64k --filename="${FUSE_FILE}" --bs=64K --rw=randread --direct=1 --ioengine=psync --iodepth=1 --runtime=30 --time_based | grep -E "READ:|bw=|IOPS="

echo ""
echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
