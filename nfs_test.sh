#!/bin/bash

# NFSv4 基础功能测试脚本
# 测试路径：/mnt/curvine-nfs/flink/user/

set -e

TEST_DIR="/mnt/curvine-nfs/flink/user"
TEST_FILE="$TEST_DIR/test_file.txt"
TEST_DIR2="$TEST_DIR/test_subdir"

echo "=========================================="
echo "NFSv4 基础功能测试"
echo "测试目录: $TEST_DIR"
echo "=========================================="
echo ""

# 测试1: 检查目录是否可访问
echo "测试1: 检查目录访问..."
if [ -d "$TEST_DIR" ]; then
    echo "✓ 目录存在且可访问"
else
    echo "✗ 目录不存在或不可访问"
    exit 1
fi
echo ""

# 测试2: 列出目录内容
echo "测试2: 列出目录内容..."
ls -la "$TEST_DIR" || echo "✗ 列出目录失败"
echo ""

# 测试3: 创建文件
echo "测试3: 创建文件..."
touch "$TEST_FILE" && echo "✓ 文件创建成功" || echo "✗ 文件创建失败"
echo ""

# 测试4: 写入数据
echo "测试4: 写入数据..."
echo "Hello NFSv4!" > "$TEST_FILE" && echo "✓ 数据写入成功" || echo "✗ 数据写入失败"
echo ""

# 测试5: 读取数据
echo "测试5: 读取数据..."
CONTENT=$(cat "$TEST_FILE")
if [ "$CONTENT" = "Hello NFSv4!" ]; then
    echo "✓ 数据读取成功: $CONTENT"
else
    echo "✗ 数据读取失败或内容不匹配"
    echo "  期望: Hello NFSv4!"
    echo "  实际: $CONTENT"
fi
echo ""

# 测试6: 追加数据
echo "测试6: 追加数据..."
echo "Second line" >> "$TEST_FILE" && echo "✓ 数据追加成功" || echo "✗ 数据追加失败"
cat "$TEST_FILE"
echo ""

# 测试7: 查看文件属性
echo "测试7: 查看文件属性..."
stat "$TEST_FILE" || echo "✗ 获取文件属性失败"
echo ""

# 测试8: 创建子目录
echo "测试8: 创建子目录..."
mkdir -p "$TEST_DIR2" && echo "✓ 子目录创建成功" || echo "✗ 子目录创建失败"
echo ""

# 测试9: 在子目录中创建文件
echo "测试9: 在子目录中创建文件..."
echo "File in subdir" > "$TEST_DIR2/subfile.txt" && echo "✓ 子目录文件创建成功" || echo "✗ 子目录文件创建失败"
cat "$TEST_DIR2/subfile.txt"
echo ""

# 测试10: 删除文件
echo "测试10: 删除文件..."
rm "$TEST_FILE" && echo "✓ 文件删除成功" || echo "✗ 文件删除失败"
echo ""

# 测试11: 删除子目录
echo "测试11: 删除子目录..."
rm -rf "$TEST_DIR2" && echo "✓ 子目录删除成功" || echo "✗ 子目录删除失败"
echo ""

echo "=========================================="
echo "测试完成！"
echo "=========================================="
