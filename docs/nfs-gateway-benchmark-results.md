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
| 顺序读 (direct) - 优化前 | psync | 1M | 1 | Yes | 352 | 352 | 3.2ms | ❌ 基线 |
| 顺序读 (direct) - 优化后 | psync | 1M | 1 | Yes | **533** | 533 | 3.0ms | ✅ +51% |
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
- **顺序读 (533 MiB/s)**: ✅ 优化后性能提升 51%，已达到合理水平
  - 优化方案：移除 NfsReader 的 channel 串行化机制
  - 当前瓶颈：单线程 psync，iodepth=1
  - 进一步优化方向：增加 IO 深度、使用 libaio
- **随机读 (8.5 MiB/s)**: 小块随机读取，受限于网络延迟

## 待优化项

1. **顺序读性能进一步优化** - 当前 533 MiB/s，目标 > 1000 MiB/s
   - [ ] 测试 libaio + iodepth=16 的性能
   - [ ] 实现预读取 (readahead)
   - [ ] 优化网络传输层

2. **随机读性能优化** - 当前 8.5 MiB/s
   - [ ] 实现读取缓存
   - [ ] 优化小块读取合并

---
*报告生成时间: 2025-12-30 12:15*
*最后更新: 2025-12-30 12:15 (优化 1: 移除 NfsReader Channel)*
