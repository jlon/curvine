# Curvine NFS Gateway 性能测试报告

## 测试环境

| 项目 | 配置 |
|------|------|
| 测试日期 | 2025-12-30 |
| 操作系统 | Linux (Ubuntu) |
| NFS 版本 | NFSv4.0 |
| FIO 版本 | 3.36 |
| 挂载点 | /mnt/curvine-nfs |
| 挂载参数 | vers=4.0,rsize=1048576,wsize=1048576,hard,proto=tcp |

## 测试结果汇总

### 写入性能

| 测试场景 | IO引擎 | 块大小 | IO深度 | Direct | 带宽 (MiB/s) | IOPS | 延迟P99 |
|---------|--------|--------|--------|--------|-------------|------|---------|
| 顺序写 (buffered) | psync | 1M | 1 | No | **2223** | 2223 | 611μs |
| 顺序写 (direct) | psync | 1M | 1 | Yes | **879** | 879 | 4.7ms |
| 高并发写 | libaio | 512K | 16 | Yes | **1747** | 3512 | 16.5ms |
| 极高并发写 | libaio | 512K | 64 | Yes | **871** | 1741 | 74ms |
| 随机写 | psync | 4K | 8 | No | **468** | 120K | 2.9ms |

### 读取性能

| 测试场景 | IO引擎 | 块大小 | IO深度 | Direct | 带宽 (MiB/s) | IOPS | 延迟P99 | 优化状态 |
|---------|--------|--------|--------|--------|-------------|------|---------|---------|
| 顺序读 (direct) - 基线 | psync | 1M | 1 | Yes | 352 | 352 | 3.2ms | ❌ V1 |
| 顺序读 (direct) - 优化1 | psync | 1M | 1 | Yes | 533 | 533 | 3.0ms | ✅ V2 +51% |
| 顺序读 (direct) - 优化2 | psync | 1M | 1 | Yes | **654** | 654 | 1.5ms | ✅ V3 +86% |
| 随机读 | psync | 4K | 8 | Yes | **8.5** | 2123 | - | - |

### 混合读写性能

| 测试场景 | 读写比 | 块大小 | IO深度 | 读带宽 | 写带宽 |
|---------|--------|--------|--------|--------|--------|
| 混合读写 | 70/30 | 4K | 8 | 9.5 MiB/s | 4 MiB/s |

## 性能优化记录

### 优化 1: 移除 NfsReader Channel 机制 (2025-12-30)

**问题分析：**
- 每个 NfsReader 内部使用 AsyncChannel 串行化所有读请求
- 虽然 ReaderPool 有 8 个 NfsReader，但每个 NfsReader 一次只能处理 1 个请求
- 导致顺序读性能仅为 352 MiB/s

**优化方案：**
- 移除 NfsReader 的 channel 机制
- 直接使用 `Arc<Mutex<UnifiedReader>>` 封装
- 保持 ReaderPool 的 8 个并发读取器

**优化结果：**
- 顺序读性能：352 MiB/s → 533 MiB/s
- 性能提升：**+51.4%** 🎉
- 延迟降低：3.2ms → 3.0ms (P99)

**代码变更：**
- `curvine-nfs/src/gateway/nfs_reader.rs`: 简化为直接封装 UnifiedReader
- `curvine-nfs/src/gateway/io_cache.rs`: 移除 Runtime 参数
- `curvine-nfs/src/nfs4/fs.rs`: 更新 ReaderPool 创建调用

### 优化 2: 移除 NfsReader 内部 Mutex (2025-12-30)

**问题分析：**
- 优化 1 后，每个 NfsReader 仍然使用 `Arc<Mutex<UnifiedReader>>`
- 虽然移除了 channel，但 Mutex 仍然串行化了每个 reader 内部的访问
- ReaderPool 的 8 个 reader 之间可以并发，但每个 reader 内部仍然是串行的

**架构演进：**
```
V1 (352 MiB/s):  NFS READ → ReaderPool → NfsReader[AsyncChannel] → UnifiedReader
V2 (533 MiB/s):  NFS READ → ReaderPool → NfsReader[Arc<Mutex<UnifiedReader>>]
V3 (654 MiB/s):  NFS READ → ReaderPool → tokio::Mutex<NfsReader> → UnifiedReader (直接拥有)
```

**核心洞察：**
- ReaderPool 有 8 个 NfsReader，每个应该独立拥有自己的 UnifiedReader
- NfsReader 内部不共享 = 不需要内部锁 = 零锁开销
- 在 ReaderEntry 层面使用 tokio::Mutex 进行异步友好的锁定

**优化方案：**
1. **NfsReader 直接拥有 UnifiedReader**
   - 从 `Arc<Mutex<UnifiedReader>>` 改为直接所有权
   - 每个 NfsReader 独占其 UnifiedReader
   - 移除 Clone trait（每个 reader 是唯一的）

2. **在 ReaderEntry 层面加锁**
   - 新增 `ReaderEntry` 结构体，包含 `tokio::sync::Mutex<NfsReader>`
   - ReaderPool 存储 `Vec<Arc<ReaderEntry>>` 而非 `Vec<Arc<NfsReader>>`
   - 在池级别锁定，而非 reader 内部锁定

3. **更新调用路径**
   - NFSv4: `OpenFile::read()` 在 ReaderEntry 级别锁定
   - NFSv3: `curvine_nfs_fs.rs` 读取路径在 entry 级别锁定

**优化结果：**
- 顺序读性能：533 MiB/s → 654 MiB/s
- 性能提升：**+22.7%** 🎉
- 延迟大幅降低：3.0ms → 1.5ms (P99)
- 相比基线总提升：**+85.8%** 🚀

**性能分析：**
- 移除了一层锁定（NfsReader 内部的 Mutex）
- 每个 NfsReader 对其拥有的 UnifiedReader 零锁开销
- 锁定仅发生在池选择级别（tokio::Mutex 是异步友好的）
- 更好的缓存局部性（每个 reader 拥有自己的数据）

**代码变更：**
- `curvine-nfs/src/gateway/nfs_reader.rs`: 直接拥有 UnifiedReader，移除内部 Mutex
- `curvine-nfs/src/gateway/io_cache.rs`: 新增 ReaderEntry，在 entry 级别加锁
- `curvine-nfs/src/nfs4/fs.rs`: 在 ReaderEntry 级别锁定 reader
- `curvine-nfs/src/gateway/curvine_nfs_fs.rs`: NFSv3 路径在 entry 级别锁定

## 详细测试命令

### 场景 1: 顺序写 (buffered)
```bash
fio --name=seq-write --filename=/mnt/curvine-nfs/fio_test \
    --size=100M --bs=1M --rw=write --direct=0 \
    --ioengine=psync --iodepth=1 --runtime=30 --time_based
```

### 场景 2: 顺序写 (direct)
```bash
fio --name=seq-write-direct --filename=/mnt/curvine-nfs/fio_test \
    --size=100M --bs=1M --rw=write --direct=1 \
    --ioengine=psync --iodepth=1 --runtime=30 --time_based
```

### 场景 3: 高并发写 (libaio)
```bash
fio --name=high-write --filename=/mnt/curvine-nfs/fio_test \
    --size=200M --bs=512K --rw=write --direct=1 \
    --ioengine=libaio --iodepth=16 --runtime=30 --time_based
```

### 场景 4: 极高并发写 (libaio)
```bash
fio --name=extreme-write --filename=/mnt/curvine-nfs/fio_test \
    --size=200M --bs=512K --rw=write --direct=1 \
    --ioengine=libaio --iodepth=64 --runtime=30 --time_based
```

### 场景 5: 顺序读 (direct)
```bash
# 先创建测试文件
dd if=/dev/zero of=/mnt/curvine-nfs/fio_read_source bs=1M count=100

fio --name=seq-read --filename=/mnt/curvine-nfs/fio_read_source \
    --bs=1M --rw=read --direct=1 \
    --ioengine=psync --iodepth=1 --runtime=30 --time_based
```

### 场景 6: 随机读
```bash
fio --name=rand-read --filename=/mnt/curvine-nfs/fio_read_source \
    --bs=4K --rw=randread --direct=1 \
    --ioengine=psync --iodepth=8 --runtime=30 --time_based
```

### 场景 7: 随机写
```bash
fio --name=rand-write --filename=/mnt/curvine-nfs/fio_test \
    --size=100M --bs=4K --rw=randwrite --direct=0 \
    --ioengine=psync --iodepth=8 --runtime=30 --time_based
```

### 场景 8: 混合读写
```bash
fio --name=mixed --filename=/mnt/curvine-nfs/fio_test \
    --size=100M --bs=4K --rw=randrw --rwmixread=70 --direct=0 \
    --ioengine=psync --iodepth=8 --runtime=30 --time_based
```

## 性能分析

### 写入性能分析
- **Buffered 写入 (2223 MiB/s)**: 性能优秀，得益于内核页缓存
- **Direct 写入 (879 MiB/s)**: 绕过缓存直接写入，性能稳定
- **高并发写入 (1747 MiB/s)**: libaio 异步 IO 表现良好

### 读取性能分析
- **顺序读 (654 MiB/s)**: ✅ 两次优化后性能提升 86%，接近目标
  - 优化 1：移除 NfsReader 的 channel 串行化机制 (+51%)
  - 优化 2：移除 NfsReader 内部 Mutex，直接拥有 UnifiedReader (+23%)
  - 当前瓶颈：单线程 psync，iodepth=1
  - 进一步优化方向：
    - 检查数据拷贝次数（零拷贝优化）
    - 验证底层 curvine-client 预读机制是否生效
    - 考虑增加 IO 深度、使用 libaio
- **随机读 (8.5 MiB/s)**: 小块随机读取，受限于网络延迟

## 待优化项

1. **顺序读性能进一步优化** - 当前 654 MiB/s，目标 > 1000 MiB/s
   - [x] 移除 NfsReader Channel 机制 ✅ (+51%)
   - [x] 移除 NfsReader 内部 Mutex ✅ (+23%)
   - [ ] 分析数据拷贝路径，实现零拷贝优化
   - [ ] 验证底层 curvine-client 预读机制是否生效
   - [ ] 测试 libaio + iodepth=16 的性能
   - [ ] 优化网络传输层

2. **随机读性能优化** - 当前 8.5 MiB/s
   - [ ] 实现读取缓存
   - [ ] 优化小块读取合并

---
*报告生成时间: 2025-12-30 12:15*
*最后更新: 2025-12-30 14:48 (优化 2: 移除 NfsReader 内部 Mutex)*
