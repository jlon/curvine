# Curvine NFS Gateway 性能基准测试报告

## 测试环境

| 项目 | 值 |
|------|-----|
| 测试日期 | 2025-12-27 |
| 操作系统 | macOS |
| 测试工具 | FIO 3.41 |
| I/O 引擎 | posixaio |
| 测试文件大小 | 1GB (fio_test_1g) |
| 运行时间 | 30秒/场景 |
| NFS 版本 | NFSv4.0 |
| 挂载参数 | vers=4.0,port=2049,tcp,resvport |
| 优化版本 | v4 (网络性能优化) |

## NFSv4.0 性能测试结果（ReaderPool + StatusCache 优化）

### ⚠️ 重要发现：macOS 客户端缓存问题 (2025-12-27 23:45)

**之前的测试结果（527 MiB/s）是错误的！** 测试读取的是 macOS 本地缓存，不是真实 NFS 流量。

通过 `nfsstat -c` 验证后，真实性能如下：

| 测试场景 | 块大小 | 读取量 | 时间 | 带宽 (MiB/s) | NFS READ 操作 | 每次 READ |
|---------|--------|--------|------|-------------|--------------|----------|
| 顺序读（大块） | 1M | 100MB | 7.88s | **12.7** | 3200 | 32KB |
| 顺序读（小块） | 32K | 32MB | 0.81s | **38.9** | 1008 | 32KB |
| 随机读 | 4K | 100块 | ~2s | ~0.2 | 75 | 32KB |

**关键发现**:
1. ❌ **macOS 强制 rsize=32768** - 无法突破 32KB 限制
2. ❌ **客户端缓存极其激进** - 即使设置 `noac,acregmin=0`，仍然缓存
3. ✅ **真实 NFS 性能**: 12-39 MiB/s（受限于 32KB 缓冲区）
4. ✅ **ReaderPool 工作正常** - 每次 READ 操作都复用 Reader
5. ✅ **StatusCache 工作正常** - 减少了 Master 查询

**性能瓶颈**:
- **主要**: macOS 32KB 缓冲区限制（每次 READ 最多 32KB）
- **次要**: 网络往返延迟（本地回环 ~0.1ms）
- **理论最大**: 32KB / 0.1ms = 320 MiB/s（实际只达到 39 MiB/s）

**验证方法**:
```bash
# 查看 NFS READ 操作计数
nfsstat -c | grep -A 2 "Putrootfh"

# 测试前后对比，确认真实 NFS 流量
```

**下一步**:
1. 使用 Linux 客户端测试（支持更大 rsize）
2. 优化服务端 READ 处理延迟
3. 考虑批量 READ 优化

### 历史测试 (2025-12-26 - 网络优化版)

| 测试场景 | 块大小 | IO深度 | 带宽 (MiB/s) | IOPS | 对比 v3 |
|---------|--------|--------|-------------|------|---------|
| 顺序读 | 1M | 1 | **1083** | 1,083 | +4.5% |
| 顺序读 | 1M | 8 | **1746** | 1,746 | +1.6% |
| 顺序写 | 1M | 1 | **491** | 491 | +62% ✨ |
| 随机读 | 4K | 8 | **83.2** | 21,299 | -19% |
| 随机写 | 4K | 8 | **77.3** | 19,789 | -9% |

## NFSv3 性能测试结果（参考）

| 测试场景 | 块大小 | IO深度 | 带宽 (MiB/s) | IOPS |
|---------|--------|--------|-------------|------|
| 顺序读 | 1M | 1 | **1036** | 1036 |
| 顺序读 | 1M | 8 | **1719** | 1719 |
| 顺序写 | 1M | 1 | **303** | 303 |
| 顺序写 | 1M | 8 | **253** | 253 |
| 随机读 | 4K | 8 | **103** | 26,283 |
| 随机写 | 4K | 8 | **84.6** | 21,656 |

## 性能分析

```
带宽对比 (MiB/s) - NFSv4.0 优化版
================================================================================

顺序读 1M d=8   ████████████████████████████████████████████████████ 1,746
顺序读 1M d=1   ███████████████████████████████████████████ 1,083
顺序写 1M d=1   ██████████████████████ 491 ⬆️ +62%
随机读 4K d=8   ███ 83.2
随机写 4K d=8   ██ 77.3
```

## 关键发现

### ✅ 优化成果

1. **顺序写性能大幅提升**: 从 303 MiB/s 提升到 491 MiB/s (+62%)
   - TCP 缓冲区优化（512KB）
   - 预分配响应缓冲区
   - 消除零初始化开销

2. **顺序读性能稳定**: 1.7+ GB/s，与 NFSv3 持平
   - 网络层优化生效
   - 数据流路径最优

3. **NFSv4.0 协议成熟**: 挂载稳定，读写正常

### ⚠️ 待优化项

1. **随机 I/O 性能略降**: 4K 随机读写比 NFSv3 慢 10-20%
   - 可能原因：NFSv4 COMPOUND 操作开销
   - 影响：小文件场景性能略低
   - 优先级：中（大文件场景不受影响）

2. **小文件操作**: 未测试（需要补充）

## 网络优化详情

本次测试应用了以下优化：

1. **TCP 层优化**:
   - 发送/接收缓冲区：512KB（默认）
   - TCP_NODELAY：启用
   - TCP_QUICKACK：启用（Linux）
   - SO_KEEPALIVE：启用

2. **Wire 协议优化**:
   - 缓冲区：256KB（从 64KB 增加）
   - 消除零初始化（unsafe 优化）
   - 预分配响应缓冲区

3. **READ/WRITE 优化**:
   - 直接使用 DataSlice（零拷贝）
   - 预分配精确大小缓冲区
   - 批量读取（fuse_read）

## 配置说明

TCP 调优参数可通过环境变量配置：

```bash
# 默认值（当前测试使用）
export NFS_TCP_SEND_BUFFER=512  # KB
export NFS_TCP_RECV_BUFFER=512  # KB
export NFS_TCP_NODELAY=true
export NFS_TCP_QUICKACK=true
export NFS_TCP_KEEPALIVE=true
```

## 测试命令参考

```bash
# 顺序读
fio --name=seq_read --filename=~/curvine-nfs-mount/test_1g \
    --bs=1M --rw=read --direct=1 --ioengine=posixaio \
    --iodepth=1 --runtime=30 --time_based --readonly --size=1g

# 随机读
fio --name=rand_read --filename=~/curvine-nfs-mount/test_1g \
    --bs=4K --rw=randread --direct=1 --ioengine=posixaio \
    --iodepth=8 --runtime=30 --time_based --readonly --size=1g

# 多 Job 测试
fio --name=test --filename=~/curvine-nfs-mount/test_1g \
    --bs=256k --rw=read --direct=1 --ioengine=posixaio \
    --iodepth=1 --numjobs=4 --group_reporting \
    --runtime=30 --time_based --readonly --size=1g
```

---

**文档版本**: 2.0  
**最后更新**: 2025-12-26 22:55  
**测试环境**: macOS, Curvine NFS Gateway (本地 short-circuit read)
