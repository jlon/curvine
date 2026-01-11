# NFSv4.1 Docker 测试指南 (macOS 环境)

本文档描述如何在 macOS 上使用 Docker 容器测试 NFSv4.1 协议实现。

## 1. 环境概述

```
┌─────────────────────────────────────────────────────────────────┐
│                        macOS Host                                │
│  ┌─────────────────────┐      ┌─────────────────────────────┐   │
│  │   curvine-nfs       │      │     Docker Desktop          │   │
│  │   (NFS Server)      │      │  ┌───────────────────────┐  │   │
│  │   Port: 2049        │◄────►│  │   Ubuntu 22.04        │  │   │
│  │                     │      │  │   (NFS Client)        │  │   │
│  └─────────────────────┘      │  │   Container: nfs41-test│  │   │
│                               │  └───────────────────────┘  │   │
│                               └─────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 1.1 为什么使用 Docker？

macOS 原生不支持挂载 NFSv4.1 文件系统（仅支持到 NFSv4.0）。通过 Docker 运行 Linux 容器，可以使用 Linux 内核的完整 NFSv4.1 客户端实现进行测试。

### 1.2 环境要求

- macOS (测试环境: macOS with Docker Desktop)
- Docker Desktop for Mac
- curvine-nfs-gateway 服务

## 2. 测试环境搭建

### 2.1 创建测试容器

```bash
# 创建一个持久运行的 Ubuntu 容器
docker run -d \
  --name nfs41-test \
  --privileged \
  ubuntu:22.04 \
  sleep infinity
```

**参数说明：**
- `--privileged`: 必须！NFS 挂载需要特权模式
- `sleep infinity`: 保持容器持续运行

### 2.2 安装 NFS 客户端工具

```bash
# 进入容器安装必要工具
docker exec nfs41-test apt-get update
docker exec nfs41-test apt-get install -y nfs-common tcpdump iproute2
```

### 2.3 验证容器网络

```bash
# 测试容器到宿主机的连通性
docker exec nfs41-test ping -c 3 host.docker.internal

# 测试 NFS 端口连通性
docker exec nfs41-test bash -c "echo > /dev/tcp/host.docker.internal/2049 && echo 'NFS port OK'"
```

## 3. NFS 服务端配置

### 3.1 启动 curvine-nfs-gateway

```bash
# 构建
cargo build --release -p curvine-nfs

# 复制到部署目录
cp target/release/curvine-nfs-gateway /path/to/curvine/build/dist/lib/

# 启动服务
/path/to/curvine/build/dist/bin/curvine-nfs-gateway.sh start

# 停止服务
/path/to/curvine/build/dist/bin/curvine-nfs-gateway.sh stop
```

### 3.2 验证服务状态

```bash
# 检查端口监听
lsof -i :2049

# 查看日志
tail -f /path/to/curvine/build/dist/logs/nfs-gateway.log
```

## 4. NFS 挂载测试

### 4.1 NFSv4.1 挂载

```bash
# 创建挂载点
docker exec nfs41-test mkdir -p /mnt/nfs41

# 挂载 NFSv4.1
docker exec nfs41-test mount -t nfs4 \
  -o vers=4.1,port=2049 \
  host.docker.internal:/ /mnt/nfs41

# 验证挂载
docker exec nfs41-test mount | grep nfs
```

**预期输出：**
```
host.docker.internal:/ on /mnt/nfs41 type nfs4 (rw,relatime,vers=4.1,rsize=1047672,wsize=1047532,namlen=255,hard,proto=tcp,timeo=600,retrans=2,sec=sys,clientaddr=172.17.0.3,local_lock=none,addr=192.168.65.2)
```

### 4.2 NFSv4.0 挂载（对比测试）

```bash
# 创建挂载点
docker exec nfs41-test mkdir -p /mnt/nfs40

# 挂载 NFSv4.0
docker exec nfs41-test mount -t nfs4 \
  -o vers=4.0,port=2049 \
  host.docker.internal:/ /mnt/nfs40
```

### 4.3 卸载

```bash
docker exec nfs41-test umount /mnt/nfs41
docker exec nfs41-test umount /mnt/nfs40
```

## 5. 功能测试

### 5.1 基本文件操作测试

```bash
# touch 命令测试（计时）
docker exec nfs41-test bash -c "time touch /mnt/nfs41/test_file.txt"

# 写入测试
docker exec nfs41-test bash -c "echo 'hello world' > /mnt/nfs41/test_write.txt"

# 读取测试
docker exec nfs41-test cat /mnt/nfs41/test_write.txt

# 目录操作
docker exec nfs41-test mkdir /mnt/nfs41/test_dir
docker exec nfs41-test ls -la /mnt/nfs41/

# 删除测试
docker exec nfs41-test rm /mnt/nfs41/test_file.txt
docker exec nfs41-test rmdir /mnt/nfs41/test_dir
```

### 5.2 性能对比测试

```bash
# NFSv4.1 touch 性能
echo "=== NFSv4.1 Touch Test ==="
for i in {1..5}; do
  docker exec nfs41-test bash -c "time touch /mnt/nfs41/perf_test_$i.txt" 2>&1
done

# NFSv4.0 touch 性能（如果已挂载）
echo "=== NFSv4.0 Touch Test ==="
for i in {1..5}; do
  docker exec nfs41-test bash -c "time touch /mnt/nfs40/perf_test_$i.txt" 2>&1
done
```

### 5.3 批量文件测试

```bash
# 创建 100 个文件
docker exec nfs41-test bash -c "
  cd /mnt/nfs41
  time for i in \$(seq 1 100); do
    touch batch_file_\$i.txt
  done
"

# 清理
docker exec nfs41-test bash -c "rm /mnt/nfs41/batch_file_*.txt"
```

## 6. 网络抓包分析

### 6.1 在容器内抓包

```bash
# 启动抓包（后台运行）
docker exec -d nfs41-test tcpdump -i eth0 -w /tmp/nfs41.pcap port 2049

# 执行测试操作
docker exec nfs41-test touch /mnt/nfs41/capture_test.txt

# 停止抓包
docker exec nfs41-test pkill tcpdump

# 复制抓包文件到宿主机
docker cp nfs41-test:/tmp/nfs41.pcap ./nfs41.pcap
```

### 6.2 使用 Wireshark 分析

1. 用 Wireshark 打开 `nfs41.pcap`
2. 过滤器: `nfs`
3. 关注以下操作序列:
   - EXCHANGE_ID
   - CREATE_SESSION
   - SEQUENCE
   - OPEN / CLOSE
   - SETATTR / GETATTR

### 6.3 实时抓包分析

```bash
# 实时显示 NFS 流量
docker exec nfs41-test tcpdump -i eth0 -nn port 2049

# 详细显示（包含数据）
docker exec nfs41-test tcpdump -i eth0 -nn -X port 2049
```

## 7. 调试技巧

### 7.1 查看 NFS 客户端状态

```bash
# 查看 RPC 统计
docker exec nfs41-test cat /proc/net/rpc/nfs

# 查看挂载信息
docker exec nfs41-test cat /proc/mounts | grep nfs

# 查看 NFS 参数
docker exec nfs41-test cat /sys/module/nfs/parameters/max_session_slots
```

### 7.2 启用 NFS 调试日志

```bash
# 启用 RPC 调试（需要 root）
docker exec nfs41-test bash -c "echo 65535 > /proc/sys/sunrpc/nfs_debug"

# 查看内核日志
docker exec nfs41-test dmesg | tail -50

# 关闭调试
docker exec nfs41-test bash -c "echo 0 > /proc/sys/sunrpc/nfs_debug"
```

### 7.3 服务端日志

```bash
# 实时查看 NFS 服务端日志
tail -f /path/to/curvine/build/dist/logs/nfs-gateway.log

# 过滤特定操作
tail -f /path/to/curvine/build/dist/logs/nfs-gateway.log | grep -E "(OPEN|CLOSE|SEQUENCE)"
```

## 8. 常见问题排查

### 8.1 挂载失败

**问题**: `mount.nfs4: Connection refused`

**解决**:
```bash
# 检查服务是否运行
lsof -i :2049

# 检查防火墙
sudo pfctl -s rules
```

### 8.2 权限问题

**问题**: `Permission denied`

**解决**:
```bash
# 确保容器以特权模式运行
docker run --privileged ...
```

### 8.3 touch 命令延迟

**问题**: touch 命令需要 5+ 秒

**排查步骤**:
1. 抓包分析请求/响应时间
2. 检查服务端日志中的 SEQUENCE status_flags
3. 对比 NFSv4.0 和 NFSv4.1 的行为差异

## 9. 测试脚本

### 9.1 完整测试脚本

```bash
#!/bin/bash
# test_nfs41.sh - NFSv4.1 完整测试脚本

CONTAINER="nfs41-test"
MOUNT_POINT="/mnt/nfs41"
NFS_SERVER="host.docker.internal"

echo "=== NFSv4.1 Test Suite ==="

# 1. 检查容器
echo "[1/6] Checking container..."
docker ps | grep $CONTAINER || {
    echo "Container not running!"
    exit 1
}

# 2. 检查挂载
echo "[2/6] Checking mount..."
docker exec $CONTAINER mount | grep $MOUNT_POINT || {
    echo "Mounting NFSv4.1..."
    docker exec $CONTAINER mount -t nfs4 -o vers=4.1,port=2049 $NFS_SERVER:/ $MOUNT_POINT
}

# 3. Touch 测试
echo "[3/6] Touch test..."
docker exec $CONTAINER bash -c "time touch $MOUNT_POINT/test_\$(date +%s).txt"

# 4. 读写测试
echo "[4/6] Read/Write test..."
docker exec $CONTAINER bash -c "echo 'test data' > $MOUNT_POINT/rw_test.txt"
docker exec $CONTAINER cat $MOUNT_POINT/rw_test.txt

# 5. 目录测试
echo "[5/6] Directory test..."
docker exec $CONTAINER mkdir -p $MOUNT_POINT/test_dir_$$
docker exec $CONTAINER ls -la $MOUNT_POINT/
docker exec $CONTAINER rmdir $MOUNT_POINT/test_dir_$$

# 6. 清理
echo "[6/6] Cleanup..."
docker exec $CONTAINER rm -f $MOUNT_POINT/test_*.txt $MOUNT_POINT/rw_test.txt

echo "=== Test Complete ==="
```

### 9.2 性能基准测试

```bash
#!/bin/bash
# benchmark_nfs41.sh - NFSv4.1 性能基准测试

CONTAINER="nfs41-test"
MOUNT_41="/mnt/nfs41"
MOUNT_40="/mnt/nfs40"
ITERATIONS=10

echo "=== NFSv4.1 vs NFSv4.0 Benchmark ==="

# NFSv4.1 测试
echo ""
echo "--- NFSv4.1 Touch Benchmark ($ITERATIONS iterations) ---"
total_41=0
for i in $(seq 1 $ITERATIONS); do
    result=$(docker exec $CONTAINER bash -c "time touch $MOUNT_41/bench_$i.txt" 2>&1 | grep real | awk '{print $2}')
    echo "  Iteration $i: $result"
done

# NFSv4.0 测试（如果已挂载）
if docker exec $CONTAINER mount | grep -q "$MOUNT_40"; then
    echo ""
    echo "--- NFSv4.0 Touch Benchmark ($ITERATIONS iterations) ---"
    for i in $(seq 1 $ITERATIONS); do
        result=$(docker exec $CONTAINER bash -c "time touch $MOUNT_40/bench_$i.txt" 2>&1 | grep real | awk '{print $2}')
        echo "  Iteration $i: $result"
    done
fi

# 清理
docker exec $CONTAINER bash -c "rm -f $MOUNT_41/bench_*.txt $MOUNT_40/bench_*.txt 2>/dev/null"

echo ""
echo "=== Benchmark Complete ==="
```

## 10. 容器管理

### 10.1 容器生命周期

```bash
# 启动容器
docker start nfs41-test

# 停止容器
docker stop nfs41-test

# 重启容器
docker restart nfs41-test

# 删除容器
docker rm -f nfs41-test
```

### 10.2 进入容器交互

```bash
# 进入容器 shell
docker exec -it nfs41-test bash

# 以 root 身份进入
docker exec -it -u root nfs41-test bash
```

## 11. 附录

### 11.1 NFSv4.1 vs NFSv4.0 主要差异

| 特性 | NFSv4.0 | NFSv4.1 |
|------|---------|---------|
| Session | 无 | 有 (CREATE_SESSION) |
| Slot/Sequence | 无 | 有 (SEQUENCE) |
| 并行操作 | 有限 | 多 slot 并行 |
| Backchannel | 可选 | 标准支持 |
| pNFS | 无 | 支持 |

### 11.2 关键 NFS 操作

- `EXCHANGE_ID`: 客户端注册，获取 client_id
- `CREATE_SESSION`: 创建会话，协商 slot 数量
- `SEQUENCE`: 每个 COMPOUND 请求的第一个操作
- `RECLAIM_COMPLETE`: 通知服务器恢复完成
- `BIND_CONN_TO_SESSION`: 绑定连接到会话

### 11.3 参考资料

- RFC 5661: NFSv4.1 Protocol
- RFC 7530: NFSv4.0 Protocol
- Linux Kernel NFS Client Source: `fs/nfs/`
- NFS-Ganesha Source: `src/Protocols/NFS/`
