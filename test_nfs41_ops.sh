#!/bin/bash
# NFSv4.1 基础文件操作全面测试脚本
# 测试所有基础操作的功能和性能

MOUNT_POINT="/mnt/curvine-nfs41"
TEST_DIR="$MOUNT_POINT/test_ops_$(date +%s)"
RESULTS_FILE="/tmp/nfs41_test_results.txt"

echo "========================================" | tee $RESULTS_FILE
echo "NFSv4.1 基础操作测试 - $(date)" | tee -a $RESULTS_FILE
echo "========================================" | tee -a $RESULTS_FILE

# 检查挂载点
if ! mountpoint -q $MOUNT_POINT; then
    echo "ERROR: $MOUNT_POINT 未挂载" | tee -a $RESULTS_FILE
    echo "请先执行: sudo mount -t nfs -o vers=4.1,port=2049,tcp,resvport 127.0.0.1:/ $MOUNT_POINT"
    exit 1
fi

echo ""
echo "=== 1. MKDIR 创建目录 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo mkdir -p "$TEST_DIR"
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "MKDIR: ${duration}s" | tee -a $RESULTS_FILE
if [ -d "$TEST_DIR" ]; then
    echo "MKDIR: PASS" | tee -a $RESULTS_FILE
else
    echo "MKDIR: FAIL" | tee -a $RESULTS_FILE
fi

echo ""
echo "=== 2. TOUCH 创建文件 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo touch "$TEST_DIR/file1.txt"
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "TOUCH: ${duration}s" | tee -a $RESULTS_FILE
if [ -f "$TEST_DIR/file1.txt" ]; then
    echo "TOUCH: PASS" | tee -a $RESULTS_FILE
else
    echo "TOUCH: FAIL" | tee -a $RESULTS_FILE
fi

echo ""
echo "=== 3. WRITE 写入文件 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
echo "Hello NFSv4.1 Test Content" | sudo tee "$TEST_DIR/file1.txt" > /dev/null
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "WRITE: ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 4. READ 读取文件 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
content=$(sudo cat "$TEST_DIR/file1.txt")
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "READ: ${duration}s" | tee -a $RESULTS_FILE
if [ "$content" = "Hello NFSv4.1 Test Content" ]; then
    echo "READ: PASS (content matches)" | tee -a $RESULTS_FILE
else
    echo "READ: FAIL (content mismatch: '$content')" | tee -a $RESULTS_FILE
fi

echo ""
echo "=== 5. GETATTR 获取属性 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo stat "$TEST_DIR/file1.txt" > /dev/null
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "GETATTR (stat): ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 6. SETATTR 设置属性 (chmod) ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo chmod 755 "$TEST_DIR/file1.txt"
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "SETATTR (chmod): ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 7. SETATTR 设置属性 (chown) ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo chown root:root "$TEST_DIR/file1.txt"
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "SETATTR (chown): ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 8. SETATTR 设置时间 (touch -t) ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo touch -t 202501010000 "$TEST_DIR/file1.txt"
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "SETATTR (touch -t): ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 9. LOOKUP 查找文件 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo ls "$TEST_DIR/file1.txt" > /dev/null
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "LOOKUP: ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 10. READDIR 读取目录 ===" | tee -a $RESULTS_FILE
# 先创建多个文件
for i in {1..10}; do
    sudo touch "$TEST_DIR/readdir_test_$i.txt"
done
time_start=$(date +%s.%N)
sudo ls -la "$TEST_DIR" > /dev/null
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "READDIR: ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 11. RENAME 重命名文件 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo mv "$TEST_DIR/file1.txt" "$TEST_DIR/file1_renamed.txt"
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "RENAME: ${duration}s" | tee -a $RESULTS_FILE
if [ -f "$TEST_DIR/file1_renamed.txt" ] && [ ! -f "$TEST_DIR/file1.txt" ]; then
    echo "RENAME: PASS" | tee -a $RESULTS_FILE
else
    echo "RENAME: FAIL" | tee -a $RESULTS_FILE
fi

echo ""
echo "=== 12. LINK 硬链接 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo ln "$TEST_DIR/file1_renamed.txt" "$TEST_DIR/file1_hardlink.txt" 2>&1
link_result=$?
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "LINK: ${duration}s (exit code: $link_result)" | tee -a $RESULTS_FILE

echo ""
echo "=== 13. SYMLINK 软链接 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo ln -s "$TEST_DIR/file1_renamed.txt" "$TEST_DIR/file1_symlink.txt" 2>&1
symlink_result=$?
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "SYMLINK: ${duration}s (exit code: $symlink_result)" | tee -a $RESULTS_FILE

echo ""
echo "=== 14. READLINK 读取软链接 ===" | tee -a $RESULTS_FILE
if [ -L "$TEST_DIR/file1_symlink.txt" ]; then
    time_start=$(date +%s.%N)
    sudo readlink "$TEST_DIR/file1_symlink.txt"
    time_end=$(date +%s.%N)
    duration=$(echo "$time_end - $time_start" | bc)
    echo "READLINK: ${duration}s" | tee -a $RESULTS_FILE
else
    echo "READLINK: SKIP (symlink not created)" | tee -a $RESULTS_FILE
fi

echo ""
echo "=== 15. REMOVE 删除文件 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo rm "$TEST_DIR/readdir_test_1.txt"
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "REMOVE: ${duration}s" | tee -a $RESULTS_FILE
if [ ! -f "$TEST_DIR/readdir_test_1.txt" ]; then
    echo "REMOVE: PASS" | tee -a $RESULTS_FILE
else
    echo "REMOVE: FAIL" | tee -a $RESULTS_FILE
fi

echo ""
echo "=== 16. RMDIR 删除目录 ===" | tee -a $RESULTS_FILE
sudo mkdir "$TEST_DIR/subdir_to_remove"
time_start=$(date +%s.%N)
sudo rmdir "$TEST_DIR/subdir_to_remove"
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "RMDIR: ${duration}s" | tee -a $RESULTS_FILE
if [ ! -d "$TEST_DIR/subdir_to_remove" ]; then
    echo "RMDIR: PASS" | tee -a $RESULTS_FILE
else
    echo "RMDIR: FAIL" | tee -a $RESULTS_FILE
fi

echo ""
echo "=== 17. ACCESS 权限检查 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo test -r "$TEST_DIR/file1_renamed.txt"
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "ACCESS: ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 18. COMMIT 同步写入 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo sync
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "COMMIT (sync): ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 19. 大文件写入测试 (1MB) ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo dd if=/dev/zero of="$TEST_DIR/largefile.bin" bs=1M count=1 2>/dev/null
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "WRITE 1MB: ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 20. 大文件读取测试 (1MB) ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo dd if="$TEST_DIR/largefile.bin" of=/dev/null bs=1M 2>/dev/null
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "READ 1MB: ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "=== 21. 多次 touch 测试 (检查延迟) ===" | tee -a $RESULTS_FILE
total_time=0
for i in {1..5}; do
    time_start=$(date +%s.%N)
    sudo touch "$TEST_DIR/touch_test_$i.txt"
    time_end=$(date +%s.%N)
    duration=$(echo "$time_end - $time_start" | bc)
    total_time=$(echo "$total_time + $duration" | bc)
    echo "  touch #$i: ${duration}s" | tee -a $RESULTS_FILE
done
avg_time=$(echo "scale=4; $total_time / 5" | bc)
echo "TOUCH avg: ${avg_time}s" | tee -a $RESULTS_FILE
if (( $(echo "$avg_time > 1.0" | bc -l) )); then
    echo "WARNING: touch 平均时间超过 1 秒，可能存在延迟问题!" | tee -a $RESULTS_FILE
fi

echo ""
echo "=== 22. 清理测试目录 ===" | tee -a $RESULTS_FILE
time_start=$(date +%s.%N)
sudo rm -rf "$TEST_DIR"
time_end=$(date +%s.%N)
duration=$(echo "$time_end - $time_start" | bc)
echo "CLEANUP: ${duration}s" | tee -a $RESULTS_FILE

echo ""
echo "========================================" | tee -a $RESULTS_FILE
echo "测试完成 - $(date)" | tee -a $RESULTS_FILE
echo "结果保存在: $RESULTS_FILE" | tee -a $RESULTS_FILE
echo "========================================" | tee -a $RESULTS_FILE
