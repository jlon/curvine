#!/bin/bash
# 基础NFS功能测试 - Mac本地版本
# 测试所有基础文件操作

set -e

# 配置参数
MOUNT_POINT="/Users/jianglong/curvine-nfs"
TEST_DIR="$MOUNT_POINT/basic_test_$(date +%s)"

# 颜色输出
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}✓${NC} $1"
}

error() {
    echo -e "${RED}✗${NC} $1"
}

warn() {
    echo -e "${YELLOW}!${NC} $1"
}

# 清理测试环境
cleanup() {
    log "清理测试环境..."
    rm -rf "$TEST_DIR" 2>/dev/null || true
}

trap cleanup EXIT

# 检查NFS是否已挂载
if ! mount | grep -q "$MOUNT_POINT"; then
    error "NFS未挂载到 $MOUNT_POINT"
    exit 1
fi

success "NFS已挂载: $(mount | grep curvine | head -1)"

# 创建测试目录
log "创建测试目录: $TEST_DIR"
mkdir -p "$TEST_DIR"

echo ""
echo "======================================"
echo " NFS基础功能测试"
echo "======================================"
echo "测试目录: $TEST_DIR"
echo "======================================"
echo ""

# 测试计数器
total_tests=0
passed_tests=0
failed_tests=0

run_test() {
    local test_name="$1"
    local test_cmd="$2"

    total_tests=$((total_tests + 1))
    log "测试 $total_tests: $test_name"

    if eval "$test_cmd" 2>/dev/null; then
        success "$test_name"
        passed_tests=$((passed_tests + 1))
    else
        error "$test_name"
        failed_tests=$((failed_tests + 1))
    fi
    return 0  # Always continue to next test
}

# 1. 文件创建测试
run_test "创建空文件" "touch '$TEST_DIR/empty_file.txt'"

# 2. 文件写入测试
run_test "写入小文件" "echo 'Hello World' > '$TEST_DIR/hello.txt'"
run_test "验证文件内容" "grep -q 'Hello World' '$TEST_DIR/hello.txt'"

# 3. 文件读取测试
run_test "读取文件" "cat '$TEST_DIR/hello.txt' > /dev/null"

# 4. 文件追加测试
run_test "追加内容" "echo 'Second line' >> '$TEST_DIR/hello.txt'"
run_test "验证追加内容" "[ \$(wc -l < '$TEST_DIR/hello.txt') -eq 2 ]"

# 5. 大文件写入测试
run_test "写入1MB文件" "dd if=/dev/zero of='$TEST_DIR/large_1mb.dat' bs=1024 count=1024 2>/dev/null"
run_test "验证文件大小" "[ \$(stat -f%z '$TEST_DIR/large_1mb.dat') -eq 1048576 ]"

# 6. 目录操作测试
run_test "创建子目录" "mkdir '$TEST_DIR/subdir'"
run_test "在子目录创建文件" "echo 'test' > '$TEST_DIR/subdir/file.txt'"
run_test "列出目录内容" "ls '$TEST_DIR/subdir' | grep -q 'file.txt'"

# 7. 文件重命名测试
run_test "文件重命名" "mv '$TEST_DIR/hello.txt' '$TEST_DIR/renamed.txt'"
run_test "验证重命名后文件存在" "[ -f '$TEST_DIR/renamed.txt' ]"
run_test "验证原文件不存在" "[ ! -f '$TEST_DIR/hello.txt' ]"

# 8. 文件复制测试
run_test "文件复制" "cp '$TEST_DIR/renamed.txt' '$TEST_DIR/copied.txt'"
run_test "验证复制后文件存在" "[ -f '$TEST_DIR/copied.txt' ]"
run_test "验证两文件内容相同" "cmp -s '$TEST_DIR/renamed.txt' '$TEST_DIR/copied.txt'"

# 9. 文件删除测试
run_test "删除文件" "rm '$TEST_DIR/copied.txt'"
run_test "验证文件已删除" "[ ! -f '$TEST_DIR/copied.txt' ]"

# 10. 目录删除测试
run_test "删除非空目录" "rm -rf '$TEST_DIR/subdir'"
run_test "验证目录已删除" "[ ! -d '$TEST_DIR/subdir' ]"

# 11. 文件权限测试
run_test "修改文件权限" "chmod 644 '$TEST_DIR/renamed.txt'"
run_test "验证文件权限" "[ \$(stat -f%Lp '$TEST_DIR/renamed.txt') = '644' ]"

# 12. 文件属性测试
run_test "获取文件状态" "stat '$TEST_DIR/renamed.txt' > /dev/null"

# 13. 符号链接测试
run_test "创建符号链接" "ln -s '$TEST_DIR/renamed.txt' '$TEST_DIR/symlink.txt'"
run_test "验证符号链接" "[ -L '$TEST_DIR/symlink.txt' ]"
run_test "通过符号链接读取" "cat '$TEST_DIR/symlink.txt' > /dev/null"

# 14. 硬链接测试
run_test "创建硬链接" "ln '$TEST_DIR/renamed.txt' '$TEST_DIR/hardlink.txt'"
run_test "验证硬链接" "[ -f '$TEST_DIR/hardlink.txt' ]"
run_test "验证链接数" "[ \$(stat -f%l '$TEST_DIR/renamed.txt') -eq 2 ]"

# 15. 文件截断测试
run_test "截断文件" "truncate -s 10 '$TEST_DIR/empty_file.txt'"
run_test "验证截断后大小" "[ \$(stat -f%z '$TEST_DIR/empty_file.txt') -eq 10 ]"

# 16. 并发写入测试
run_test "并发写入5个文件" "
    for i in {1..5}; do
        echo \"test \$i\" > '$TEST_DIR/concurrent_\$i.txt' &
    done
    wait
"
run_test "验证并发写入结果" "[ \$(ls '$TEST_DIR'/concurrent_*.txt | wc -l) -eq 5 ]"

# 17. 小文件批量创建测试
run_test "批量创建10个小文件" "
    for i in {1..10}; do
        echo \"file \$i\" > '$TEST_DIR/batch_\$i.txt'
    done
"
run_test "验证批量创建结果" "[ \$(ls '$TEST_DIR'/batch_*.txt | wc -l) -eq 10 ]"

# 18. 文件覆盖写入测试
run_test "覆盖写入文件" "echo 'Overwritten' > '$TEST_DIR/renamed.txt'"
run_test "验证覆盖内容" "grep -q 'Overwritten' '$TEST_DIR/renamed.txt'"

# 19. 零字节文件测试
run_test "创建零字节文件" ": > '$TEST_DIR/zero_byte.txt'"
run_test "验证零字节文件" "[ \$(stat -f%z '$TEST_DIR/zero_byte.txt') -eq 0 ]"

# 20. 目录嵌套测试
run_test "创建嵌套目录" "mkdir -p '$TEST_DIR/a/b/c/d'"
run_test "在嵌套目录创建文件" "echo 'deep' > '$TEST_DIR/a/b/c/d/deep.txt'"
run_test "验证嵌套文件" "[ -f '$TEST_DIR/a/b/c/d/deep.txt' ]"

echo ""
echo "======================================"
echo " 测试结果汇总"
echo "======================================"
echo "总测试数:   $total_tests"
echo -e "通过:       ${GREEN}$passed_tests${NC}"
echo -e "失败:       ${RED}$failed_tests${NC}"
echo "通过率:     $(awk "BEGIN {printf \"%.2f%%\", ($passed_tests/$total_tests)*100}")"
echo "======================================"

if [ $failed_tests -eq 0 ]; then
    success "所有测试通过！"
    exit 0
else
    error "有 $failed_tests 个测试失败"
    exit 1
fi
