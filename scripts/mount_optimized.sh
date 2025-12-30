#!/bin/bash
# Optimized NFS Mount Script for Performance Testing
# 
# Performance optimizations:
# - async: Enable asynchronous writes (better performance)
# - noatime: Don't update access time (reduces metadata operations)
# - nodiratime: Don't update directory access time
# - rsize/wsize: 1MB transfer size (already optimal)
# - timeo=150: Reduce timeout from 600 to 150 (1.5 seconds)
# - retrans=3: Increase retransmissions for reliability
# - actimeo=600: Attribute cache timeout (10 minutes)

set -e

MOUNT_POINT="/mnt/curvine-nfs"
NFS_SERVER="127.0.0.1"
NFS_EXPORT="/"

echo "=========================================="
echo "Optimized NFS Mount"
echo "=========================================="
echo "Server: $NFS_SERVER"
echo "Export: $NFS_EXPORT"
echo "Mount Point: $MOUNT_POINT"
echo ""

# Unmount if already mounted
if mountpoint -q "$MOUNT_POINT"; then
    echo "Unmounting existing mount..."
    sudo umount "$MOUNT_POINT"
    sleep 1
fi

# Mount with optimized parameters
echo "Mounting with optimized parameters..."
sudo mount -t nfs \
    -o vers=4.0,rsize=1048576,wsize=1048576,hard,proto=tcp,timeo=150,retrans=3,async,noatime,nodiratime,actimeo=600 \
    "$NFS_SERVER:$NFS_EXPORT" "$MOUNT_POINT"

echo ""
echo "Mount successful!"
echo ""

# Show mount options
echo "Current mount options:"
nfsstat -m

echo ""
echo "=========================================="
echo "Mount Complete"
echo "=========================================="
