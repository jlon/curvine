#!/usr/bin/env python3
"""Test ftruncate on NFS mount to verify SETATTR stateid fix."""

import os
import sys

NFS_MOUNT = os.path.expanduser("~/nfs4_mount")
TEST_FILE = os.path.join(NFS_MOUNT, "test_ftruncate.dat")

def test_ftruncate():
    """Test ftruncate operation."""
    print(f"Testing ftruncate on {TEST_FILE}")
    
    # Clean up
    if os.path.exists(TEST_FILE):
        os.remove(TEST_FILE)
    
    # Open file for write
    print("1. Opening file for write...")
    fd = os.open(TEST_FILE, os.O_CREAT | os.O_RDWR, 0o644)
    
    # Write some data
    print("2. Writing 1KB of data...")
    os.write(fd, b'A' * 1024)
    
    # Truncate to 512 bytes (this triggers SETATTR with stateid)
    print("3. Truncating to 512 bytes (ftruncate)...")
    try:
        os.ftruncate(fd, 512)
        print("   ✅ ftruncate succeeded!")
    except OSError as e:
        print(f"   ❌ ftruncate failed: {e}")
        os.close(fd)
        return False
    
    # Verify size
    print("4. Verifying file size...")
    stat = os.fstat(fd)
    print(f"   File size: {stat.st_size}")
    
    if stat.st_size == 512:
        print("   ✅ Size is correct!")
    else:
        print(f"   ❌ Size mismatch: expected 512, got {stat.st_size}")
        os.close(fd)
        return False
    
    # Close file
    os.close(fd)
    
    # Verify size after close
    print("5. Verifying size after close...")
    stat = os.stat(TEST_FILE)
    print(f"   File size: {stat.st_size}")
    
    if stat.st_size == 512:
        print("   ✅ Size is correct after close!")
    else:
        print(f"   ❌ Size mismatch after close: expected 512, got {stat.st_size}")
        return False
    
    # Clean up
    os.remove(TEST_FILE)
    print("\n✅ All ftruncate tests passed!")
    return True

if __name__ == "__main__":
    success = test_ftruncate()
    sys.exit(0 if success else 1)
