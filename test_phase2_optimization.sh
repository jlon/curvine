#!/bin/bash
# Phase 2 小文件优化 - 完整测试脚本

set -e

echo "================================================="
echo "  Phase 2 小文件异步Flush优化 - 完整测试"
echo "================================================="

# 1. 检查NFS挂载
MOUNT_POINT="$HOME/curvine-nfs"
if ! mount | grep -q "$MOUNT_POINT"; then
    echo "错误: $MOUNT_POINT 未挂载"
    echo "请先执行:"
    echo "  sudo mkdir -p ~/curvine-nfs"
    echo "  sudo mount -t nfs -o vers=4.0,port=2049,tcp,resvport 127.0.0.1:/ ~/curvine-nfs"
    exit 1
fi

echo "✓ NFS挂载点: $MOUNT_POINT"

# 2. 清空NFS日志
LOG_FILE="/Users/jianglong/IdeaProjects/curvine/build/dist/logs/curvine-nfs-gateway.out"
echo "清空日志..."
> "$LOG_FILE"

# 3. 准备测试目录
TEST_DIR="$MOUNT_POINT/phase2_test_$(date +%s)"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"
echo "✓ 测试目录: $TEST_DIR"

# 4. 测试1: 单个小文件 (完整流程: OPEN -> WRITE -> CLOSE)
echo -e "\n【测试1: 单个小文件 (50KB)】"
dd if=/dev/zero of="$TEST_DIR/single.dat" bs=51200 count=1 2>/dev/null
sync
sleep 1

# 5. 测试2: 多个小文件
echo -e "\n【测试2: 10个小文件 (100KB each)】"
for i in {1..10}; do
    dd if=/dev/zero of="$TEST_DIR/file_$i.dat" bs=102400 count=1 2>/dev/null
    if [ $((i % 5)) -eq 0 ]; then
        echo "  进度: $i/10"
    fi
done
sync
sleep 2

# 6. 分析日志
echo -e "\n================================================="
echo "  日志分析"
echo "================================================="

echo -e "\n【WRITE操作统计】"
WRITE_COUNT=$(grep "PERF_NFSWRITER_WRITE" "$LOG_FILE" | wc -l | tr -d ' ')
echo "总WRITE操作: $WRITE_COUNT"

echo -e "\n【FLUSH操作统计】"
FLUSH_COUNT=$(grep "PERF_NFSWRITER_FLUSH" "$LOG_FILE" | wc -l | tr -d ' ')
echo "总FLUSH操作: $FLUSH_COUNT"
echo "期望: 0 (所有WRITE应该跳过flush)"

echo -e "\n【小文件识别】"
DECISION_COUNT=$(grep "SmallFile: DECISION" "$LOG_FILE" | wc -l | tr -d ' ')
echo "决策次数: $DECISION_COUNT"
echo "最近3条决策:"
grep "SmallFile: DECISION" "$LOG_FILE" | tail -3

echo -e "\n【CLOSE操作】"
CLOSE_COUNT=$(grep "CLOSE.*fileid=" "$LOG_FILE" | grep -v "\.json" | wc -l | tr -d ' ')
echo "CLOSE操作: $CLOSE_COUNT"

echo -e "\n【异步Flush】"
ASYNC_FLUSH_COUNT=$(grep "SmallFile.*Async flush on CLOSE" "$LOG_FILE" | wc -l | tr -d ' ')
echo "异步flush触发: $ASYNC_FLUSH_COUNT"

BACKGROUND_FLUSH_COUNT=$(grep "PERF_BACKGROUND_FLUSH" "$LOG_FILE" | wc -l | tr -d ' ')
echo "后台flush完成: $BACKGROUND_FLUSH_COUNT"

if [ "$BACKGROUND_FLUSH_COUNT" -gt 0 ]; then
    echo -e "\n后台flush性能:"
    grep "PERF_BACKGROUND_FLUSH" "$LOG_FILE" | tail -5
fi

# 7. 结论
echo -e "\n================================================="
echo "  测试结论"
echo "================================================="

if [ "$FLUSH_COUNT" -eq 0 ]; then
    echo "✅ WRITE阶段flush跳过: 成功 ($WRITE_COUNT个WRITE, 0个FLUSH)"
else
    echo "❌ WRITE阶段flush跳过: 失败 (还有$FLUSH_COUNT个FLUSH)"
fi

if [ "$ASYNC_FLUSH_COUNT" -gt 0 ]; then
    echo "✅ CLOSE阶段异步flush: 成功 (触发$ASYNC_FLUSH_COUNT次)"
else
    echo "⚠️  CLOSE阶段异步flush: 未观察到 (可能文件未关闭或日志未捕获)"
fi

echo -e "\n完整日志文件: $LOG_FILE"
echo "================================================="
