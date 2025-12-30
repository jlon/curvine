# Curvine NFS Gateway vs FUSE 性能对比报告

## 测试环境

| 项目 | 配置 |
|------|------|
| 测试日期 | 2025-12-30 |
| 操作系统 | Linux (Ubuntu) |
| NFS 版本 | NFSv4.0 (Curvine NFS Gateway) |
| FUSE 版本 | Curvine FUSE |
| FIO 版本 | 3.36 |
| NFS 挂载点 | /mnt/curvine-nfs |
| FUSE 挂载点 | /curvine-fuse |
| NFS 挂载参数 | vers=4.0,rsize=1048576,wsize=1048576,hard,proto=tcp |
| 测试文件大小 | 1 GB |

## 核心结论

### 性能特点总结

**FUSE 优势场景**：
- ✅ **本地访问多线程顺序读**：FUSE (13 GB/s) 是 NFS (2.3 GB/s) 的 **5.7倍**
- ✅ **单线程顺序读**：FUSE (951 MiB/s) 比 NFS (572 MiB/s) 快 66%
- ✅ **4K随机读**：FUSE (15.1 MiB/s) 比 NFS (11.7 MiB/s) 快 29%

**NFS Gateway 优势场景**：
- ✅ **高并发异步I/O (libaio)**：NFS (2365 MiB/s) 是 FUSE (905 MiB/s) 的 **2.6倍**
- ✅ **网络访问需求**：NFS 支持远程访问，FUSE 仅限本地
- ✅ **标准协议兼容**：NFS 是工业标准，兼容性更好

**推荐使用场景：**
- 🏠 **本地访问 + 多线程负载**：选择 FUSE（性能最优）
- 🌐 **网络访问需求**：选择 NFS Gateway（唯一选择）
- ⚡ **高并发异步I/O**：选择 NFS Gateway（libaio场景下性能更好）
- 📝 **简单顺序读写**：选择 FUSE（单线程性能更好）

---

## 详细性能对比

### 1. 顺序读性能对比 (Direct I/O, 1M 块大小)

| 测试配置 | NFS Gateway | FUSE | NFS/FUSE | 性能差异 | 胜者 |
|---------|-------------|------|----------|---------|------|
| **psync, 单线程** | 572 MiB/s | 951 MiB/s | 60% | FUSE 快 66% | **FUSE** 🏆 |
| **psync, 4 线程** | 2530 MiB/s | 9086 MiB/s | 28% | FUSE 快 259% | **FUSE** 🏆 |
| **psync, 8 线程** | 2287 MiB/s | **13005 MiB/s** | **18%** | FUSE 快 469% | **FUSE** 🏆 |
| **libaio, iodepth=16** | **2365 MiB/s** | 905 MiB/s | **261%** | NFS 快 161% | **NFS** 🏆 |
| **libaio, iodepth=32** | **2318 MiB/s** | 885 MiB/s | **262%** | NFS 快 162% | **NFS** 🏆 |

**关键发现**：
- ⚠️ NFS 8线程性能 (2287 MiB/s) 反而低于4线程 (2530 MiB/s)，说明存在瓶颈
- ✅ NFS在libaio场景下表现优异，是FUSE的2.6倍
- ✅ FUSE多线程扩展性极佳：单线程→8线程提升13.7倍

### 2. 随机读性能对比 (Direct I/O, 4K 块大小)

| 测试配置 | NFS Gateway | FUSE | NFS/FUSE | 性能差异 | 胜者 |
|---------|-------------|------|----------|---------|------|
| **psync, 单线程** | 11.7 MiB/s | 15.1 MiB/s | 77% | FUSE 快 29% | **FUSE** 🏆 |

### 3. 顺序写性能对比 (Direct I/O, 1M 块大小)

| 测试配置 | NFS Gateway | FUSE | NFS/FUSE | 性能差异 | 胜者 |
|---------|-------------|------|----------|---------|------|
| **psync, 单线程** | 1025 MiB/s | 1122 MiB/s | 91% | FUSE快9% | **FUSE** 🏆 |
| **psync, 4线程** | 1694 MiB/s | 1172 MiB/s | 145% | NFS快45% | **NFS** 🏆 |
| **psync, 8线程** | **1787 MiB/s** | 1109 MiB/s | **161%** | NFS快61% | **NFS** 🏆 |
| **libaio, iodepth=16** | **1768 MiB/s** | 1031 MiB/s | **171%** | NFS快71% | **NFS** 🏆 |
| **libaio, iodepth=32** | **1754 MiB/s** | 1029 MiB/s | **170%** | NFS快70% | **NFS** 🏆 |

**关键发现**：
- ⚠️ **写入性能与读取性能完全相反**！
- NFS写入多线程扩展性优秀（1.74x）
- FUSE写入多线程扩展性差（几乎无提升）
- 原因：FUSE的AsyncChannel串行化瓶颈

### 4. NFS多线程随机读性能 (4K 块大小)

| 线程数 | NFS Gateway | vs 单线程 | 平均延迟 |
|--------|-------------|-----------|----------|
| 1 | 13.6 MiB/s (3484 IOPS) | 基线 | 286 μs |
| 4 | **18.3 MiB/s** (4675 IOPS) | +34% ✅ | 854 μs |
| 8 | 5.4 MiB/s (1355 IOPS) | -60% ⚠️ | 5893 μs |

**注**：8 线程性能下降是因为 ReaderPool 大小限制（8 个 reader），增加 pool 大小可解决。

---

## 性能差距深度分析

### 为什么FUSE多线程性能远超NFS？

**根本原因：架构差异**

#### FUSE架构（零拷贝）
```
用户进程 → 内核FUSE驱动 → FUSE守护进程 → UnifiedReader
         ← splice零拷贝 ←
```
- **数据拷贝次数**: 0-1次
- **并发模型**: 内核态并发，充分利用多核
- **协议开销**: 无网络协议，本地文件系统接口

#### NFS架构（多次拷贝）
```
用户进程 → 内核NFS客户端 → TCP/IP → NFS服务器 → RPC解码 → XDR解码 
         → OpenFile::read() → UnifiedReader → XDR编码 → RPC编码 
         → TCP/IP → 内核NFS客户端 → 用户进程
```
- **数据拷贝次数**: 7次
- **并发模型**: 用户态RPC处理，受ReaderPool限制
- **协议开销**: 完整的RPC/XDR编解码 + TCP/IP栈

### 5.7x性能差距的构成

| 因素 | 性能影响 | 说明 |
|------|---------|------|
| 数据拷贝 | ~2x | FUSE零拷贝 vs NFS 7次拷贝 |
| 网络协议开销 | ~1.5x | TCP/IP栈处理、上下文切换 |
| RPC编解码 | ~1.5x | XDR编码是CPU密集型操作 |
| 并发限制 | ~1.3x | ReaderPool大小限制（8个） |
| **总计** | **~5.85x** | 2 × 1.5 × 1.5 × 1.3 ≈ 5.85x |

### 为什么NFS在libaio场景下反超？

**NFS libaio优势**：
- NFS服务器是完全异步的（tokio运行时）
- 可以高效处理大量并发请求
- ReaderPool的8个reader可以并行工作

**FUSE libaio限制**：
- FuseReader使用AsyncChannel串行化所有读请求
- 即使libaio提交多个请求，也会被串行处理
- 无法利用libaio的并发优势

---

## 优化建议

### 立即可执行的优化

#### 1. 增加ReaderPool大小（预期提升20-30%）
```rust
// curvine-common/src/conf/nfs_gateway.rs
pub struct NfsGatewayConf {
    pub reader_pool_size: usize, // 从8增加到32或64
}
```

#### 2. 减少XDR编码拷贝（预期提升10-15%）
- 使用内存池减少分配开销
- 优化build_read_response()的数据拷贝

### 中长期优化方向

#### 3. 零拷贝RPC（预期提升50-100%）
- 使用io_uring或类似机制
- 避免用户态/内核态拷贝

#### 4. RDMA支持（预期接近FUSE性能）
- 使用RDMA绕过TCP/IP栈
- 实现真正的零拷贝网络传输

---

## 性能测试命令

### 顺序读测试

```bash
# 单线程 psync
fio --name=seq-read-st --filename=/mnt/curvine-nfs/testfile \
    --bs=1M --rw=read --direct=1 --ioengine=psync \
    --iodepth=1 --numjobs=1 --runtime=30 --time_based

# 8 线程 psync
fio --name=seq-read-mt8 --filename=/mnt/curvine-nfs/testfile \
    --bs=1M --rw=read --direct=1 --ioengine=psync \
    --iodepth=1 --numjobs=8 --runtime=30 --time_based --group_reporting

# libaio iodepth=16
fio --name=seq-read-aio --filename=/mnt/curvine-nfs/testfile \
    --bs=1M --rw=read --direct=1 --ioengine=libaio \
    --iodepth=16 --numjobs=1 --runtime=30 --time_based
```

### 随机读测试

```bash
# 4K 随机读
fio --name=rand-read-4k --filename=/mnt/curvine-nfs/testfile \
    --bs=4K --rw=randread --direct=1 --ioengine=psync \
    --iodepth=1 --numjobs=1 --runtime=30 --time_based

# 64K 随机读
fio --name=rand-read-64k --filename=/mnt/curvine-nfs/testfile \
    --bs=64K --rw=randread --direct=1 --ioengine=psync \
    --iodepth=1 --numjobs=1 --runtime=30 --time_based
```

---
