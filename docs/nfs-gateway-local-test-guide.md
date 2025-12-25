# Curvine NFS Gateway 本地测试指南 (macOS)

## 1. 环境准备

### 1.1 系统要求

- **操作系统**: macOS
- **FIO 版本**: ≥ 3.16 (通过 `brew install fio` 安装)

### 1.2 创建挂载点

```bash
# NFSv3 挂载点
mkdir -p ~/curvine-nfs-mount

# NFSv4 挂载点 (新增)
mkdir -p ~/nfs4_mount
```

## 2. 服务启动

### 2.1 启动 Curvine 集群

**重要**: 必须设置环境变量 `CURVINE_MASTER_HOSTNAME` 和 `CURVINE_WORKER_HOSTNAME` 为 `localhost`，否则 Worker 会注册为错误的地址。

```bash
# 设置环境变量并启动集群
export CURVINE_MASTER_HOSTNAME=localhost
export CURVINE_WORKER_HOSTNAME=localhost
./build/dist/bin/restart-all.sh
```

### 2.2 验证 Worker 状态

等待几秒后，验证 Worker 已正确注册：

```bash
./build/dist/bin/cv node -l
```

预期输出：
```
Worker Nodes:
Address                   Status          Capacity        Available      
----------------------------------------------------------------------
localhost:8997            Live            465.7GB         18.2GB         
```

**注意**: 如果 Worker 地址显示为 `0.0.1.1:8997` 而不是 `localhost:8997`，说明环境变量未正确设置，需要重新启动。

### 2.3 启动 NFS Gateway

```bash
./build/dist/bin/curvine-nfs-gateway.sh restart
```

预期输出：
```
curvine-nfs-gateway started successfully with PID xxxxx
NFS Gateway available at: nfs://0.0.0.0:2049
```

## 3. NFSv3 挂载测试 (已验证)

### 3.1 挂载命令

```bash
sudo mount_nfs -o vers=3,tcp,port=2049,mountport=2049,rsize=1048576,wsize=1048576,resvport \
  localhost:/ ~/curvine-nfs-mount
```

### 3.2 验证挂载

```bash
mount | grep curvine-nfs
ls -la ~/curvine-nfs-mount/
```

### 3.3 卸载命令

```bash
sudo umount ~/curvine-nfs-mount
```

## 4. NFSv4 挂载测试 (正在开发中)

### 4.1 当前状态

我们正在实现 NFSv4.0 和 NFSv4.1 支持。当前进展：

- ✅ **基础协议支持**: COMPOUND 操作、XDR 编解码
- ✅ **客户端管理**: SETCLIENTID、SETCLIENTID_CONFIRM (v4.0)
- ✅ **会话管理**: EXCHANGE_ID、CREATE_SESSION (v4.1)  
- ✅ **文件操作**: OPEN、CLOSE、READ、WRITE、GETATTR
- ✅ **目录操作**: LOOKUP、READDIR、PUTROOTFH、GETFH
- ✅ **安全信息**: SECINFO 操作
- ⚠️ **当前问题**: 挂载成功但文件操作 (touch, vim) 失败

### 4.2 NFSv4.0 挂载命令

```bash
# NFSv4.0 挂载 (推荐用于测试)
sudo mount -t nfs -o vers=4.0,port=2049,tcp 127.0.0.1:/ ~/nfs4_mount
```

### 4.3 NFSv4.1 挂载命令

```bash
# NFSv4.1 挂载 (实验性)
sudo mount -t nfs -o vers=4.1,port=2049,tcp 127.0.0.1:/ ~/nfs4_mount
```

### 4.4 验证 NFSv4 挂载

```bash
# 检查挂载状态
mount | grep nfs4_mount

# 查看挂载详情
mount -v | grep nfs4_mount

# 列出根目录 (应该能成功)
ls -la ~/nfs4_mount/

# 测试基本操作 (当前会失败)
touch ~/nfs4_mount/test.txt  # 预期失败
echo "test" > ~/nfs4_mount/test.txt  # 预期失败
```

### 4.5 NFSv4 卸载命令

```bash
sudo umount ~/nfs4_mount
```

### 4.6 已知问题

1. **文件创建失败**: `touch` 和 `echo >` 操作失败，可能是 OPEN 操作的问题
2. **权限问题**: 可能与 NFSv4 的权限模型有关
3. **状态管理**: NFSv4 的 stateid 管理可能存在问题

### 4.7 调试信息

查看 NFS Gateway 日志以获取详细错误信息：

```bash
tail -f ~/IdeaProjects/curvine/logs/curvine-nfs-gateway.out
```

## 5. FIO 性能测试

### 5.1 创建测试文件

```bash
# NFSv3 测试
dd if=/dev/urandom of=~/curvine-nfs-mount/fio_rand_read bs=1M count=100

# NFSv4 测试 (一旦文件创建问题解决)
dd if=/dev/urandom of=~/nfs4_mount/fio_rand_read bs=1M count=100
```

### 5.2 随机读测试

```bash
# NFSv3
fio --name=rand-read-nfs \
    --filename=~/curvine-nfs-mount/fio_rand_read \
    --bs=4K \
    --rw=randread \
    --direct=1 \
    --ioengine=posixaio \
    --iodepth=8 \
    --runtime=30 \
    --time_based

# NFSv4 (待文件创建问题解决后测试)
fio --name=rand-read-nfs4 \
    --filename=~/nfs4_mount/fio_rand_read \
    --bs=4K \
    --rw=randread \
    --direct=1 \
    --ioengine=posixaio \
    --iodepth=8 \
    --runtime=30 \
    --time_based
```

### 5.3 顺序读测试

```bash
# NFSv3
fio --name=seq-read-nfs \
    --filename=~/curvine-nfs-mount/fio_rand_read \
    --bs=1M \
    --rw=read \
    --direct=1 \
    --ioengine=posixaio \
    --iodepth=1 \
    --runtime=30 \
    --time_based

# NFSv4 (待测试)
fio --name=seq-read-nfs4 \
    --filename=~/nfs4_mount/fio_rand_read \
    --bs=1M \
    --rw=read \
    --direct=1 \
    --ioengine=posixaio \
    --iodepth=1 \
    --runtime=30 \
    --time_based
```

### 5.4 顺序写测试

```bash
# NFSv3
fio --name=seq-write-nfs \
    --filename=~/curvine-nfs-mount/fio_seq_write \
    --size=100M \
    --bs=1M \
    --rw=write \
    --direct=1 \
    --ioengine=posixaio \
    --iodepth=1 \
    --runtime=30 \
    --time_based

# NFSv4 (待测试)
fio --name=seq-write-nfs4 \
    --filename=~/nfs4_mount/fio_seq_write \
    --size=100M \
    --bs=1M \
    --rw=write \
    --direct=1 \
    --ioengine=posixaio \
    --iodepth=1 \
    --runtime=30 \
    --time_based
```

## 6. 测试结果

### 6.1 NFSv3 测试结果 (2025-12-26)

#### 随机读测试结果

| 指标 | 值 |
|------|-----|
| IOPS | 384 |
| 带宽 | 1539 KiB/s (1.5 MB/s) |
| 平均延迟 | 20.78 ms |
| P50 延迟 | 19 ms |
| P99 延迟 | 58 ms |
| 测试时长 | 30 秒 |
| 总读取量 | 45.1 MiB |

### 6.2 NFSv4 测试结果

**状态**: 待完成 - 当前文件操作存在问题，无法进行性能测试

## 7. 开发参考

### 7.1 NFSv4 参考实现

- **GitHub**: https://github.com/jgarzik/nfs4d
- **RFC 7530**: NFSv4.0 协议规范
- **RFC 5661**: NFSv4.1 协议规范

### 7.2 当前实现文件

- `curvine-nfs/src/nfs4/handlers.rs` - NFSv4 操作处理器
- `curvine-nfs/src/nfs4/compound.rs` - COMPOUND 请求处理
- `curvine-nfs/src/nfs4/state/` - 客户端和状态管理
- `curvine-nfs/src/nfs4/fs.rs` - NFSv4 文件系统接口

## 8. 常见问题

### 8.1 NFSv3 问题

#### 挂载卡住或超时

**原因**: Master 服务未启动或 NFS Gateway 未启动

**解决方案**:
1. 检查 Master 服务: `lsof -i :8995`
2. 检查 NFS Gateway: `lsof -i :2049`
3. 重启服务: `./build/dist/bin/restart-all.sh`

#### Worker 地址显示为 0.0.1.1

**原因**: 未设置 `CURVINE_WORKER_HOSTNAME` 环境变量

**解决方案**:
```bash
export CURVINE_WORKER_HOSTNAME=localhost
./build/dist/bin/restart-all.sh
```

#### 读写时出现 I/O 错误

**原因**: 
1. Worker 不可用或被 Blacklist
2. 文件数据存储在已失效的 Worker 上

**解决方案**:
1. 检查 Worker 状态: `./build/dist/bin/cv node -l`
2. 重启集群并重新创建测试文件

#### NFS 文件大小显示为 0

**原因**: NFS 客户端缓存问题

**解决方案**: 使用 CLI 验证实际文件大小
```bash
./build/dist/bin/cv fs ls /
```

### 8.2 NFSv4 问题

#### 挂载成功但无法创建文件

**当前状态**: 已知问题，正在修复中

**可能原因**:
1. OPEN 操作实现不完整
2. Stateid 管理问题
3. 权限检查问题
4. NFSv4.0 vs NFSv4.1 协议差异

**调试方法**:
```bash
# 查看详细日志
tail -f ~/IdeaProjects/curvine/logs/curvine-nfs-gateway.out

# 使用 tcpdump 抓包分析
sudo tcpdump -i lo0 -w nfs4.pcap port 2049

# 使用 Wireshark 分析 NFS 协议
```

## 9. 服务管理命令速查

```bash
# 启动集群 (Master + Worker)
export CURVINE_MASTER_HOSTNAME=localhost
export CURVINE_WORKER_HOSTNAME=localhost
./build/dist/bin/restart-all.sh

# 启动/停止/重启 NFS Gateway
./build/dist/bin/curvine-nfs-gateway.sh start
./build/dist/bin/curvine-nfs-gateway.sh stop
./build/dist/bin/curvine-nfs-gateway.sh restart
./build/dist/bin/curvine-nfs-gateway.sh status

# 查看 Worker 状态
./build/dist/bin/cv node -l

# 查看文件系统
./build/dist/bin/cv fs ls /

# NFSv3 挂载
sudo mount_nfs -o vers=3,tcp,port=2049,mountport=2049,rsize=1048576,wsize=1048576,resvport \
  localhost:/ ~/curvine-nfs-mount

# NFSv4.0 挂载
sudo mount -t nfs -o vers=4.0,port=2049,tcp 127.0.0.1:/ ~/nfs4_mount

# NFSv4.1 挂载
sudo mount -t nfs -o vers=4.1,port=2049,tcp 127.0.0.1:/ ~/nfs4_mount

# 卸载
sudo umount ~/curvine-nfs-mount  # NFSv3
sudo umount ~/nfs4_mount         # NFSv4
```

---

**文档版本**: 2.0  
**最后更新**: 2025-12-27  
**测试环境**: macOS, Curvine NFS Gateway  
**NFSv4 状态**: 开发中 - 挂载成功，文件操作待修复
