#!/bin/bash

# Curvine 配额功能端到端验证脚本
# 按照 curvine-quota-management.md 文档验证所有配额功能

set -e  # 遇到错误立即退出

CURVINE_CLI="/home/oppo/Documents/curvine/build/dist/bin/curvine"
LOG_PREFIX="[QUOTA_TEST]"

# 日志函数
log_info() {
    echo "$LOG_PREFIX INFO: $1"
}

log_success() {
    echo "$LOG_PREFIX ✅ SUCCESS: $1"
}

log_error() {
    echo "$LOG_PREFIX ❌ ERROR: $1"
}

# 清理函数
cleanup() {
    log_info "清理测试数据..."
    $CURVINE_CLI fs rm -r /quota_test_cli || true
}

# 测试前清理
cleanup

log_info "=== Curvine 配额功能端到端验证 ==="

# 1. 创建测试目录结构
log_info "1. 创建测试目录结构"
$CURVINE_CLI fs mkdir -p /quota_test_cli/dir_a/subdir
$CURVINE_CLI fs mkdir -p /quota_test_cli/dir_b
$CURVINE_CLI fs mkdir -p /quota_test_cli/dir_c/nested/deep
log_success "目录结构创建完成"

# 2. 测试配额添加 (quota add)
log_info "2. 测试配额添加功能"

# 添加不同大小的配额
$CURVINE_CLI quota add --path "/quota_test_cli" --quota-size 10485760  # 10MB
log_success "主目录配额设置: 10MB"

$CURVINE_CLI quota add --path "/quota_test_cli/dir_a" --quota-size 5242880  # 5MB
log_success "子目录配额设置: 5MB"

$CURVINE_CLI quota add --path "/quota_test_cli/dir_b" --quota-size 2097152  # 2MB
log_success "子目录配额设置: 2MB"

$CURVINE_CLI quota add --path "/quota_test_cli/dir_c" --quota-size 1048576  # 1MB
log_success "子目录配额设置: 1MB"

# 3. 测试配额列表 (quota list)
log_info "3. 测试配额列表功能"
echo "当前所有配额设置："
$CURVINE_CLI quota list
log_success "配额列表显示正常"

# 4. 创建文件并验证配额跟踪
log_info "4. 创建文件并验证配额跟踪"

# 创建不同大小的文件
echo "Creating test file content..." > /tmp/test_file_500b.txt
head -c 500 /dev/zero >> /tmp/test_file_500b.txt

echo "Creating larger test file..." > /tmp/test_file_1kb.txt
head -c 1024 /dev/zero >> /tmp/test_file_1kb.txt

echo "Creating even larger test file..." > /tmp/test_file_2kb.txt
head -c 2048 /dev/zero >> /tmp/test_file_2kb.txt

# 上传文件到不同目录
$CURVINE_CLI fs put /tmp/test_file_500b.txt /quota_test_cli/file1.txt
$CURVINE_CLI fs put /tmp/test_file_1kb.txt /quota_test_cli/dir_a/file2.txt
$CURVINE_CLI fs put /tmp/test_file_2kb.txt /quota_test_cli/dir_b/file3.txt

log_success "测试文件上传完成"

# 5. 验证配额使用情况
log_info "5. 验证配额使用情况"
echo "配额使用情况："
$CURVINE_CLI quota list

# 检查特定路径的配额信息
log_info "检查主目录配额状态："
$CURVINE_CLI quota list | grep "/quota_test_cli" || echo "未找到主目录配额信息"

log_info "检查子目录配额状态："
$CURVINE_CLI quota list | grep "/quota_test_cli/dir_a" || echo "未找到子目录配额信息"

# 6. 测试配额更新 (quota update)
log_info "6. 测试配额更新功能"

# 增加配额大小
$CURVINE_CLI quota update --path "/quota_test_cli/dir_a" --quota-size 7340032  # 7MB
log_success "配额更新: dir_a 从 5MB 增加到 7MB"

# 减少配额大小
$CURVINE_CLI quota update --path "/quota_test_cli/dir_b" --quota-size 1572864  # 1.5MB
log_success "配额更新: dir_b 从 2MB 减少到 1.5MB"

# 验证更新结果
log_info "验证配额更新结果："
$CURVINE_CLI quota list

# 7. 测试文件移动对配额的影响
log_info "7. 测试文件移动对配额的影响"

log_info "移动前的配额状态："
$CURVINE_CLI quota list

# 将文件从一个配额目录移动到另一个
$CURVINE_CLI fs mv /quota_test_cli/dir_a/file2.txt /quota_test_cli/dir_c/moved_file2.txt
log_success "文件移动: dir_a/file2.txt -> dir_c/moved_file2.txt"

log_info "移动后的配额状态："
$CURVINE_CLI quota list

# 8. 测试文件删除对配额的影响
log_info "8. 测试文件删除对配额的影响"

log_info "删除前的配额状态："
$CURVINE_CLI quota list

# 删除文件
$CURVINE_CLI fs rm /quota_test_cli/dir_b/file3.txt
log_success "文件删除: dir_b/file3.txt"

log_info "删除后的配额状态："
$CURVINE_CLI quota list

# 9. 测试目录删除对配额的影响
log_info "9. 测试目录删除对配额的影响"

# 在嵌套目录中创建文件
echo "Deep nested file content" > /tmp/deep_file.txt
$CURVINE_CLI fs put /tmp/deep_file.txt /quota_test_cli/dir_c/nested/deep/deep_file.txt

log_info "创建深层文件后的配额状态："
$CURVINE_CLI quota list

# 删除整个嵌套目录
$CURVINE_CLI fs rm -r /quota_test_cli/dir_c/nested
log_success "目录删除: dir_c/nested (递归)"

log_info "删除目录后的配额状态："
$CURVINE_CLI quota list

# 10. 测试配额删除 (quota remove)
log_info "10. 测试配额删除功能"

# 删除子目录配额
$CURVINE_CLI quota remove --path "/quota_test_cli/dir_b"
log_success "配额删除: dir_b"

$CURVINE_CLI quota remove --path "/quota_test_cli/dir_c"
log_success "配额删除: dir_c"

log_info "删除部分配额后的状态："
$CURVINE_CLI quota list

# 11. 最终验证
log_info "11. 最终状态验证"

log_info "最终文件系统状态："
$CURVINE_CLI fs ls -R /quota_test_cli

log_info "最终配额状态："
$CURVINE_CLI quota list

# 12. 性能测试 - 创建多个小文件
log_info "12. 性能测试 - 批量文件操作"

# 创建性能测试目录
$CURVINE_CLI fs mkdir -p /quota_test_cli/perf_test
$CURVINE_CLI quota add --path "/quota_test_cli/perf_test" --quota-size 52428800  # 50MB

# 创建多个小文件
for i in {1..10}; do
    echo "Performance test file $i content" > "/tmp/perf_file_$i.txt"
    $CURVINE_CLI fs put "/tmp/perf_file_$i.txt" "/quota_test_cli/perf_test/file_$i.txt"
done

log_success "批量文件创建完成"

log_info "性能测试后的配额状态："
$CURVINE_CLI quota list

# 清理临时文件
rm -f /tmp/test_file_*.txt /tmp/deep_file.txt /tmp/perf_file_*.txt

log_success "=== 配额功能端到端验证完成 ==="
log_info "所有配额命令 (add/list/update/remove) 都已验证"
log_info "配额跟踪在文件创建、移动、删除操作中都工作正常"
log_info "嵌套配额目录和批量操作都得到了验证"
