#!/bin/bash
# NFS性能基准测试 - 使用fio（修正版）
#
# 用途：在实施UNSTABLE写优化前，记录当前性能基准
#
# 测试场景：针对小文件优化

set -e

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 配置
MOUNT_POINT="/mnt/nfs"
TEST_DIR="$MOUNT_POINT/fio_baseline_test"
RESULT_FILE="/tmp/fio_baseline_$(date +%Y%m%d_%H%M%S).txt"

echo -e "${GREEN}=================================="
echo " NFS FIO性能基准测试（修正版）"
echo " 测试时间: $(date)"
echo "==================================${NC}"

# 检查NFS挂载
if ! mountpoint -q "$MOUNT_POINT"; then
    echo -e "${RED}错误: $MOUNT_POINT 未挂载${NC}"
    exit 1
fi

# 检查fio
if ! command -v fio &> /dev/null; then
    echo -e "${YELLOW}安装fio...${NC}"
    apt-get update -qq && apt-get install -y fio > /dev/null 2>&1
fi

# 清理
echo -e "${YELLOW}清理测试环境...${NC}"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

# 初始化结果文件
cat > "$RESULT_FILE" <<EOF
====================================
NFS FIO 性能基准测试结果
====================================
测试时间: $(date)
NFS挂载点: $MOUNT_POINT
fio版本: $(fio --version)

====================================
EOF

echo -e "\n${GREEN}开始性能测试...${NC}\n"

# ============================================================================
# 测试1: 小文件顺序写 (1KB × 100个文件)
# 关键参数：filesize=1k 指定每个文件1KB
# ============================================================================
echo -e "${YELLOW}[测试1] 小文件顺序写 (1KB × 100个文件)${NC}"
echo "" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"
echo "测试1: 小文件顺序写 (1KB × 100个文件)" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"

fio --name=small_1k_write \
    --directory="$TEST_DIR" \
    --ioengine=sync \
    --rw=write \
    --bs=1k \
    --filesize=1k \
    --nrfiles=100 \
    --openfiles=100 \
    --create_on_open=1 \
    --end_fsync=0 \
    --group_reporting \
    --output-format=normal \
    2>&1 | tee -a "$RESULT_FILE"

rm -rf "$TEST_DIR"/*
echo ""

# ============================================================================
# 测试2: 小文件顺序写 (4KB × 100个文件)
# ============================================================================
echo -e "${YELLOW}[测试2] 小文件顺序写 (4KB × 100个文件)${NC}"
echo "" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"
echo "测试2: 小文件顺序写 (4KB × 100个文件)" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"

fio --name=small_4k_write \
    --directory="$TEST_DIR" \
    --ioengine=sync \
    --rw=write \
    --bs=4k \
    --filesize=4k \
    --nrfiles=100 \
    --openfiles=100 \
    --create_on_open=1 \
    --end_fsync=0 \
    --group_reporting \
    --output-format=normal \
    2>&1 | tee -a "$RESULT_FILE"

rm -rf "$TEST_DIR"/*
echo ""

# ============================================================================
# 测试3: 小文件顺序写 (64KB × 100个文件)
# ============================================================================
echo -e "${YELLOW}[测试3] 小文件顺序写 (64KB × 100个文件)${NC}"
echo "" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"
echo "测试3: 小文件顺序写 (64KB × 100个文件)" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"

fio --name=small_64k_write \
    --directory="$TEST_DIR" \
    --ioengine=sync \
    --rw=write \
    --bs=64k \
    --filesize=64k \
    --nrfiles=100 \
    --openfiles=100 \
    --create_on_open=1 \
    --end_fsync=0 \
    --group_reporting \
    --output-format=normal \
    2>&1 | tee -a "$RESULT_FILE"

rm -rf "$TEST_DIR"/*
echo ""

# ============================================================================
# 测试4: 小文件随机写 (4KB × 50个文件，每个文件16KB)
# ============================================================================
echo -e "${YELLOW}[测试4] 小文件随机写 (4KB块 × 50个16KB文件)${NC}"
echo "" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"
echo "测试4: 小文件随机写 (4KB块 × 50个16KB文件)" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"

fio --name=small_rand_write \
    --directory="$TEST_DIR" \
    --ioengine=sync \
    --rw=randwrite \
    --bs=4k \
    --filesize=16k \
    --nrfiles=50 \
    --openfiles=50 \
    --create_on_open=1 \
    --end_fsync=0 \
    --group_reporting \
    --output-format=normal \
    2>&1 | tee -a "$RESULT_FILE"

rm -rf "$TEST_DIR"/*
echo ""

# ============================================================================
# 测试5: 小文件顺序读 (1KB × 100个文件)
# ============================================================================
echo -e "${YELLOW}[测试5] 小文件顺序读 (1KB × 100个文件)${NC}"
echo "" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"
echo "测试5: 小文件顺序读 (1KB × 100个文件)" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"

# 先创建文件
fio --name=prepare_read \
    --directory="$TEST_DIR" \
    --ioengine=sync \
    --rw=write \
    --bs=1k \
    --filesize=1k \
    --nrfiles=100 \
    --openfiles=100 \
    --create_on_open=1 \
    --end_fsync=1 \
    --group_reporting \
    > /dev/null 2>&1

# 读测试
fio --name=small_1k_read \
    --directory="$TEST_DIR" \
    --ioengine=sync \
    --rw=read \
    --bs=1k \
    --filesize=1k \
    --nrfiles=100 \
    --openfiles=100 \
    --group_reporting \
    --output-format=normal \
    2>&1 | tee -a "$RESULT_FILE"

rm -rf "$TEST_DIR"/*
echo ""

# ============================================================================
# 测试6: 混合读写 (70%读/30%写, 4KB块, 50个文件)
# ============================================================================
echo -e "${YELLOW}[测试6] 混合读写 (70%读/30%写, 4KB × 50个文件)${NC}"
echo "" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"
echo "测试6: 混合读写 (70%读/30%写, 4KB × 50个文件)" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"

# 先创建文件
fio --name=prepare_rw \
    --directory="$TEST_DIR" \
    --ioengine=sync \
    --rw=write \
    --bs=4k \
    --filesize=16k \
    --nrfiles=50 \
    --openfiles=50 \
    --create_on_open=1 \
    --end_fsync=1 \
    --group_reporting \
    > /dev/null 2>&1

# 混合测试
fio --name=small_randrw \
    --directory="$TEST_DIR" \
    --ioengine=sync \
    --rw=randrw \
    --rwmixread=70 \
    --bs=4k \
    --filesize=16k \
    --nrfiles=50 \
    --openfiles=50 \
    --runtime=20s \
    --time_based \
    --group_reporting \
    --output-format=normal \
    2>&1 | tee -a "$RESULT_FILE"

rm -rf "$TEST_DIR"/*
echo ""

# ============================================================================
# 测试7: 单个大文件顺序写 (1MB, 4KB块)
# ============================================================================
echo -e "${YELLOW}[测试7] 单个大文件顺序写 (1MB, 4KB块)${NC}"
echo "" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"
echo "测试7: 单个大文件顺序写 (1MB, 4KB块)" >> "$RESULT_FILE"
echo "========================================" >> "$RESULT_FILE"

fio --name=large_file_write \
    --directory="$TEST_DIR" \
    --ioengine=sync \
    --rw=write \
    --bs=4k \
    --size=1m \
    --end_fsync=0 \
    --group_reporting \
    --output-format=normal \
    2>&1 | tee -a "$RESULT_FILE"

rm -rf "$TEST_DIR"/*
echo ""

# ============================================================================
# 生成摘要
# ============================================================================
echo -e "${GREEN}=================================="
echo " 测试完成！生成摘要..."
echo "==================================${NC}"

cat >> "$RESULT_FILE" <<EOF

====================================
性能指标摘要（提取关键IOPS/BW）
====================================

关键测试场景:
  - 测试1 (1KB写): 最关键，对应UNSTABLE优化目标
  - 测试2 (4KB写): 典型小文件场景
  - 测试3 (64KB写): 批处理上限
  - 测试5 (1KB读): 对比P0 Data Cache效果

请从上面详细结果中提取:
  - IOPS (每秒I/O操作数)
  - BW (带宽 KB/s或MB/s)
  - lat (延迟 usec/msec)

====================================
EOF

# 清理
echo -e "${YELLOW}清理测试环境...${NC}"
rm -rf "$TEST_DIR"

# 输出结果
echo -e "${GREEN}=================================="
echo " 测试结果已保存至:"
echo " $RESULT_FILE"
echo "==================================${NC}"

# 提取简单摘要（从结果文件中grep关键指标）
echo -e "\n${GREEN}快速摘要:${NC}"
echo "从结果文件提取write IOPS..."
grep -E "write:.*IOPS=" "$RESULT_FILE" | head -5 || echo "需要手动查看详细结果"
echo ""

cat <<EOF

${GREEN}下一步操作:${NC}
1. 查看完整结果: cat $RESULT_FILE
2. 复制到主机: docker cp nfs41-test:$RESULT_FILE .
3. 重点关注:
   - 测试1 write IOPS (1KB小文件写)
   - 测试2 write IOPS (4KB典型场景)
   - 测试5 read IOPS (验证Data Cache)
4. 记录基准数据后，实施UNSTABLE优化
5. 重新运行此脚本，对比性能

EOF

echo "结果文件: $RESULT_FILE"
