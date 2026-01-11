#!/bin/bash
# NFSv4.0 小文件性能测试
# 对比修复前后的性能差异

set -e

# 配置参数
MOUNT_POINT="/mnt/nfs"
TEST_DIR="$MOUNT_POINT/perf_test"
NUM_FILES=100  # 先用100个文件快速测试
FILE_SIZE="1K"

# 颜色输出
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}✓${NC} $1"
}

error() {
    echo -e "${RED}✗${NC} $1"
}

# 清理测试环境
cleanup() {
    log "清理测试环境..."
    rm -rf "$TEST_DIR" 2>/dev/null || true
}

trap cleanup EXIT

# 检查NFS是否已挂载
if ! mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
    error "NFS未挂载到 $MOUNT_POINT"
    log "请先运行: mkdir -p $MOUNT_POINT && mount -t nfs -o nfsvers=4.0,tcp host.docker.internal:/ $MOUNT_POINT"
    exit 1
fi

success "NFS已挂载: $(mount | grep nfs | head -1)"

# 创建测试目录
log "创建测试目录: $TEST_DIR"
mkdir -p "$TEST_DIR"

echo ""
echo "=================================="
echo " NFSv4.0 小文件性能测试"
echo "=================================="
echo "文件数量: $NUM_FILES"
echo "文件大小: $FILE_SIZE"
echo "测试目录: $TEST_DIR"
echo "=================================="
echo ""

# 1. 小文件写入测试
log "测试1: ${NUM_FILES}个${FILE_SIZE}文件写入"
start_time=$(date +%s.%N)
for i in $(seq 1 $NUM_FILES); do
    dd if=/dev/zero of="$TEST_DIR/file_$i.txt" bs=1024 count=1 2>/dev/null
done
end_time=$(date +%s.%N)
write_time=$(echo "$end_time - $start_time" | bc)
write_throughput=$(echo "$NUM_FILES / $write_time" | bc -l)
success "写入完成: ${write_time}秒, 吞吐量: $(printf "%.2f" $write_throughput) files/sec"

# 2. 小文件读取测试
log "测试2: ${NUM_FILES}个${FILE_SIZE}文件读取"
start_time=$(date +%s.%N)
for i in $(seq 1 $NUM_FILES); do
    cat "$TEST_DIR/file_$i.txt" > /dev/null
done
end_time=$(date +%s.%N)
read_time=$(echo "$end_time - $start_time" | bc)
read_throughput=$(echo "$NUM_FILES / $read_time" | bc -l)
success "读取完成: ${read_time}秒, 吞吐量: $(printf "%.2f" $read_throughput) files/sec"

# 3. 元数据操作测试 (stat)
log "测试3: ${NUM_FILES}个文件元数据查询(stat)"
start_time=$(date +%s.%N)
for i in $(seq 1 $NUM_FILES); do
    stat "$TEST_DIR/file_$i.txt" > /dev/null 2>&1
done
end_time=$(date +%s.%N)
stat_time=$(echo "$end_time - $start_time" | bc)
stat_throughput=$(echo "$NUM_FILES / $stat_time" | bc -l)
success "元数据查询完成: ${stat_time}秒, 吞吐量: $(printf "%.2f" $stat_throughput) ops/sec"

# 4. 小文件touch测试 (模拟原始场景)
log "测试4: touch新文件性能"
rm -rf "$TEST_DIR/touch_test"
mkdir -p "$TEST_DIR/touch_test"
start_time=$(date +%s.%N)
for i in $(seq 1 20); do  # touch测试用20个文件
    touch "$TEST_DIR/touch_test/touch_$i.txt"
done
end_time=$(date +%s.%N)
touch_time=$(echo "$end_time - $start_time" | bc)
touch_avg=$(echo "$touch_time / 20" | bc -l)
success "Touch完成: ${touch_time}秒, 平均: $(printf "%.3f" $touch_avg) sec/file"

# 5. 顺序读写测试 (混合操作)
log "测试5: 顺序读写混合操作"
rm -rf "$TEST_DIR/mixed_test"
mkdir -p "$TEST_DIR/mixed_test"
start_time=$(date +%s.%N)
for i in $(seq 1 50); do
    # 写
    echo "test data $i" > "$TEST_DIR/mixed_test/file_$i.txt"
    # 读
    cat "$TEST_DIR/mixed_test/file_$i.txt" > /dev/null
    # 查询
    stat "$TEST_DIR/mixed_test/file_$i.txt" > /dev/null 2>&1
done
end_time=$(date +%s.%N)
mixed_time=$(echo "$end_time - $start_time" | bc)
mixed_throughput=$(echo "150 / $mixed_time" | bc -l)  # 50 files * 3 ops
success "混合操作完成: ${mixed_time}秒, 吞吐量: $(printf "%.2f" $mixed_throughput) ops/sec"

echo ""
echo "=================================="
echo " 性能测试总结"
echo "=================================="
printf "写入吞吐量:       %.2f files/sec\n" $write_throughput
printf "读取吞吐量:       %.2f files/sec\n" $read_throughput
printf "元数据吞吐量:     %.2f ops/sec\n" $stat_throughput
printf "Touch平均延迟:    %.3f sec/file\n" $touch_avg
printf "混合操作吞吐量:   %.2f ops/sec\n" $mixed_throughput
echo "=================================="

# 记录结果到文件
result_file="/tmp/nfs_perf_$(date +%Y%m%d_%H%M%S).txt"
cat > "$result_file" <<EOF
NFSv4.0 Performance Test Results
Date: $(date)
================================
Configuration:
- Files: $NUM_FILES
- Size: $FILE_SIZE
- Mount: $(mount | grep nfs | head -1)

Results:
- Write:    $(printf "%.2f" $write_throughput) files/sec (${write_time}s total)
- Read:     $(printf "%.2f" $read_throughput) files/sec (${read_time}s total)
- Stat:     $(printf "%.2f" $stat_throughput) ops/sec (${stat_time}s total)
- Touch:    $(printf "%.3f" $touch_avg) sec/file (${touch_time}s for 20 files)
- Mixed:    $(printf "%.2f" $mixed_throughput) ops/sec (${mixed_time}s total)
================================
EOF

log "结果已保存到: $result_file"
