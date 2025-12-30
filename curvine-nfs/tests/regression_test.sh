#!/bin/bash
# NFSv4 Regression Test Suite
# Tests basic file operations on NFS mount point

# Configuration
NFS_MOUNT="${NFS_MOUNT:-/mnt/curvine-nfs}"
TEST_DIR="$NFS_MOUNT/regression_test_$$"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Counters
PASSED=0
FAILED=0

# Test function
run_test() {
    local name="$1"
    local cmd="$2"
    printf "  Testing: %-40s" "$name"
    if eval "$cmd" > /dev/null 2>&1; then
        echo -e " ${GREEN}✓ PASS${NC}"
        ((PASSED++))
    else
        echo -e " ${RED}✗ FAIL${NC}"
        ((FAILED++))
    fi
}

# Cleanup function
cleanup() {
    rm -rf "$TEST_DIR" 2>/dev/null || true
}

# Setup
trap cleanup EXIT
mkdir -p "$TEST_DIR"

echo "=========================================="
echo "NFSv4 Regression Test Suite"
echo "=========================================="
echo "Test Directory: $TEST_DIR"
echo ""

# Category 1: File Operations
echo "=== Test Category 1: File Operations ==="
run_test "Create regular file" "touch '$TEST_DIR/test_file.txt'"
run_test "Write to file" "echo 'Hello NFS' > '$TEST_DIR/test_file.txt'"
run_test "Read from file" "cat '$TEST_DIR/test_file.txt' | grep -q 'Hello NFS'"
run_test "Append to file" "echo 'Appended line' >> '$TEST_DIR/test_file.txt'"
run_test "Read appended content" "cat '$TEST_DIR/test_file.txt' | grep -q 'Appended line'"
run_test "File exists check" "test -f '$TEST_DIR/test_file.txt'"
run_test "File size check" "test -s '$TEST_DIR/test_file.txt'"
echo ""

# Category 2: Directory Operations
echo "=== Test Category 2: Directory Operations ==="
run_test "Create directory" "mkdir '$TEST_DIR/subdir'"
run_test "Create nested directory" "mkdir -p '$TEST_DIR/nested/deep/dir'"
run_test "List directory" "ls '$TEST_DIR' | grep -q 'subdir'"
run_test "Directory exists check" "test -d '$TEST_DIR/subdir'"
run_test "Remove empty directory" "rmdir '$TEST_DIR/nested/deep/dir'"
run_test "Remove directory" "rm -rf '$TEST_DIR/nested'"
echo ""

# Category 3: Symbolic Link Operations
echo "=== Test Category 3: Symbolic Link Operations ==="
run_test "Create symlink" "ln -s '$TEST_DIR/test_file.txt' '$TEST_DIR/symlink_file'"
run_test "Read symlink target" "readlink '$TEST_DIR/symlink_file' | grep -q 'test_file.txt'"
run_test "Access file via symlink" "cat '$TEST_DIR/symlink_file' | grep -q 'Hello NFS'"
run_test "Create symlink to directory" "ln -s '$TEST_DIR/subdir' '$TEST_DIR/symlink_dir'"
run_test "Create symlink with relative path" "ln -s '../test_file.txt' '$TEST_DIR/subdir/rel_symlink'"
run_test "Symlink exists check" "test -L '$TEST_DIR/symlink_file'"
echo ""

# Category 4: File Permissions
echo "=== Test Category 4: File Permissions ==="
run_test "Set file permissions (755)" "chmod 755 '$TEST_DIR/test_file.txt'"
run_test "Verify permissions (755)" "stat -c '%a' '$TEST_DIR/test_file.txt' | grep -q '755'"
run_test "Set file permissions (644)" "chmod 644 '$TEST_DIR/test_file.txt'"
run_test "Verify permissions (644)" "stat -c '%a' '$TEST_DIR/test_file.txt' | grep -q '644'"
run_test "Set directory permissions (700)" "chmod 700 '$TEST_DIR/subdir'"
run_test "Verify directory permissions" "stat -c '%a' '$TEST_DIR/subdir' | grep -q '700'"
echo ""

# Category 5: File Attributes
echo "=== Test Category 5: File Attributes ==="
run_test "Create file with specific mode" "touch '$TEST_DIR/mode_test.txt' && chmod 600 '$TEST_DIR/mode_test.txt'"
run_test "Verify file mode" "stat -c '%a' '$TEST_DIR/mode_test.txt' | grep -q '600'"
run_test "Create directory with specific mode" "mkdir -m 750 '$TEST_DIR/mode_dir'"
run_test "Verify directory mode" "stat -c '%a' '$TEST_DIR/mode_dir' | grep -q '750'"
echo ""

# Category 6: File Operations in Subdirectories
echo "=== Test Category 6: File Operations in Subdirectories ==="
chmod 755 "$TEST_DIR/subdir"
run_test "Create file in subdirectory" "echo 'subdir content' > '$TEST_DIR/subdir/subfile.txt'"
run_test "Read file from subdirectory" "cat '$TEST_DIR/subdir/subfile.txt' | grep -q 'subdir content'"
run_test "Create symlink in subdirectory" "ln -s '../test_file.txt' '$TEST_DIR/subdir/link_to_parent'"
run_test "Access symlink in subdirectory" "cat '$TEST_DIR/subdir/link_to_parent' | grep -q 'Hello NFS'"
echo ""

# Category 7: Multiple Operations
echo "=== Test Category 7: Multiple Operations ==="
run_test "Create multiple files" "for i in 1 2 3 4 5; do touch \"$TEST_DIR/multi_\$i.txt\"; done"
run_test "Create multiple symlinks" "for i in 1 2 3; do ln -s \"$TEST_DIR/test_file.txt\" \"$TEST_DIR/multi_link_\$i\"; done"
run_test "List all files" "ls '$TEST_DIR' | wc -l | grep -q '[0-9]'"
run_test "List all symlinks" "find '$TEST_DIR' -type l | wc -l | grep -q '[0-9]'"
echo ""

# Category 8: Rename Operations
echo "=== Test Category 8: Rename Operations ==="
run_test "Create file for rename" "echo 'rename_content' > '$TEST_DIR/rename_source.txt'"
run_test "Rename file (same dir)" "mv '$TEST_DIR/rename_source.txt' '$TEST_DIR/rename_target.txt'"
run_test "Verify renamed file exists" "test -f '$TEST_DIR/rename_target.txt'"
run_test "Verify original file removed" "! test -f '$TEST_DIR/rename_source.txt'"
run_test "Verify renamed file content" "cat '$TEST_DIR/rename_target.txt' | grep -q 'rename_content'"

# Cross-directory rename
run_test "Create dirs for cross-dir rename" "mkdir -p '$TEST_DIR/src_dir' '$TEST_DIR/dst_dir'"
run_test "Create file in source dir" "echo 'cross_dir_content' > '$TEST_DIR/src_dir/cross_file.txt'"
run_test "Cross-directory rename" "mv '$TEST_DIR/src_dir/cross_file.txt' '$TEST_DIR/dst_dir/cross_file.txt'"
run_test "Verify cross-dir rename target exists" "test -f '$TEST_DIR/dst_dir/cross_file.txt'"
run_test "Verify cross-dir rename source removed" "! test -f '$TEST_DIR/src_dir/cross_file.txt'"
run_test "Verify cross-dir renamed file content" "cat '$TEST_DIR/dst_dir/cross_file.txt' | grep -q 'cross_dir_content'"

# Directory rename
run_test "Create directory for rename" "mkdir '$TEST_DIR/dir_to_rename'"
run_test "Rename directory" "mv '$TEST_DIR/dir_to_rename' '$TEST_DIR/renamed_dir'"
run_test "Verify renamed directory exists" "test -d '$TEST_DIR/renamed_dir'"
run_test "Verify original directory removed" "! test -d '$TEST_DIR/dir_to_rename'"
echo ""

# Category 9: File Removal
echo "=== Test Category 9: File Removal ==="
run_test "Remove regular file" "rm '$TEST_DIR/rename_target.txt'"
run_test "Verify file removed" "! test -f '$TEST_DIR/rename_target.txt'"
run_test "Remove symlink" "rm '$TEST_DIR/symlink_file'"
run_test "Verify symlink removed" "! test -L '$TEST_DIR/symlink_file'"
run_test "Remove directory with files" "rm -rf '$TEST_DIR/dst_dir'"
run_test "Verify directory removed" "! test -d '$TEST_DIR/dst_dir'"
echo ""

# Category 10: Edge Cases
echo "=== Test Category 10: Edge Cases ==="
run_test "Create file with special characters" "touch \"$TEST_DIR/file_with_spaces.txt\""
run_test "Create symlink with special name" "ln -s \"$TEST_DIR/file_with_spaces.txt\" \"$TEST_DIR/link_with_spaces\""
run_test "Access symlink with spaces" "test -L \"$TEST_DIR/link_with_spaces\""
run_test "Create long filename" "touch \"$TEST_DIR/\$(printf 'a%.0s' {1..200}).txt\""
run_test "Create symlink to non-existent target" "ln -s '/nonexistent/path' \"$TEST_DIR/broken_link\""
echo ""

# Summary
echo "=========================================="
echo "Test Results Summary"
echo "=========================================="
echo "Total Tests: $((PASSED + FAILED))"
echo "Passed: $PASSED"
echo "Failed: $FAILED"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All regression tests passed! ✅${NC}"
else
    echo -e "${RED}Some tests failed! ❌${NC}"
fi

echo ""
echo "Cleaning up test files..."
