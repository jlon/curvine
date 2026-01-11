#!/bin/bash
# 部署修复后的NFS gateway并运行性能测试

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$PROJECT_ROOT/build/dist"

# 颜色输出
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
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

warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

echo "=================================="
echo " NFSv4.0 Performance Fix Deployment"
echo "=================================="
echo ""

# 1. 复制新构建的binary
log "步骤1: 复制修复后的binary到部署目录"
if [ ! -f "$PROJECT_ROOT/target/release/curvine-nfs-gateway" ]; then
    error "未找到构建的binary: target/release/curvine-nfs-gateway"
    exit 1
fi

cp "$PROJECT_ROOT/target/release/curvine-nfs-gateway" "$DIST_DIR/lib/"
success "Binary已复制到 $DIST_DIR/lib/"

# 2. 停止旧的NFS gateway
log "步骤2: 停止旧的NFS gateway进程"
OLD_PID=$(ps aux | grep '[c]urvine-nfs-gateway' | grep -v grep | awk '{print $2}' | head -1)
if [ -n "$OLD_PID" ]; then
    kill $OLD_PID
    sleep 2
    success "已停止旧进程 (PID: $OLD_PID)"
else
    warn "未找到运行中的NFS gateway"
fi

# 3. 启动新的NFS gateway
log "步骤3: 启动修复后的NFS gateway"
cd "$PROJECT_ROOT"
nohup build/dist/lib/curvine-nfs-gateway \
    --conf build/dist/conf/curvine-cluster.toml \
    --listen-addr 0.0.0.0 \
    --listen-port 2049 \
    --export-path / \
    serve > build/dist/logs/curvine-nfs-gateway.out 2>&1 &
NEW_PID=$!
sleep 3

# 检查进程是否启动
if ps -p $NEW_PID > /dev/null; then
    success "NFS gateway已启动 (PID: $NEW_PID)"
else
    error "NFS gateway启动失败"
    tail -20 build/dist/logs/curvine-nfs-gateway.out
    exit 1
fi

# 4. 在Docker容器中设置NFS挂载
log "步骤4: 在Docker容器中挂载NFS"

# 检查容器是否运行
if ! docker ps | grep -q nfs41-test; then
    error "Docker容器 nfs41-test 未运行"
    exit 1
fi

# 卸载旧挂载(如果存在)
docker exec nfs41-test bash -c "umount /mnt/nfs 2>/dev/null || true"
docker exec nfs41-test bash -c "rm -rf /mnt/nfs && mkdir -p /mnt/nfs"

# 挂载NFSv4.0
log "挂载NFSv4.0到容器..."
docker exec nfs41-test mount -t nfs -o nfsvers=4.0,tcp host.docker.internal:/ /mnt/nfs

# 验证挂载
if docker exec nfs41-test mountpoint -q /mnt/nfs; then
    success "NFS挂载成功"
    docker exec nfs41-test mount | grep nfs
else
    error "NFS挂载失败"
    exit 1
fi

echo ""
echo "=================================="
echo " 部署完成，准备运行性能测试"
echo "=================================="
echo ""

# 5. 运行性能测试
log "步骤5: 运行性能基准测试"

# 将测试脚本复制到容器
docker cp "$SCRIPT_DIR/nfs_perf_test.sh" nfs41-test:/tmp/

# 在容器中运行测试
docker exec nfs41-test bash /tmp/nfs_perf_test.sh

# 6. 提取测试结果
log "步骤6: 提取测试结果"
RESULT_FILE=$(docker exec nfs41-test ls -t /tmp/nfs_perf_*.txt 2>/dev/null | head -1)
if [ -n "$RESULT_FILE" ]; then
    docker exec nfs41-test cat "$RESULT_FILE"

    # 保存到本地
    LOCAL_RESULT="$PROJECT_ROOT/build/dist/logs/nfs_perf_$(date +%Y%m%d_%H%M%S).txt"
    docker exec nfs41-test cat "$RESULT_FILE" > "$LOCAL_RESULT"
    success "结果已保存到: $LOCAL_RESULT"
fi

echo ""
echo "=================================="
echo " 测试完成"
echo "=================================="
