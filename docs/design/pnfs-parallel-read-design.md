# pNFS 并行读取设计文档

## 0. 项目背景

### 0.1 项目概述

本项目正在模仿 [NFS-Ganesha](https://github.com/nfs-ganesha/nfs-ganesha) 的 `src/Protocols/NFS` 实现，开发 NFSv4.1 协议的适配，底层存储使用 Curvine 分布式存储系统。

### 0.2 开发环境

**参考实现**:
- NFS-Ganesha: `/home/oppo/Documents/nfs-ganesha/src/Protocols/NFS`
- 核心参考文件:
  - `src/FSAL/FSAL_GPFS/fsal_pnfs.c`: pNFS 布局管理
  - `src/support/ds.c`: Data Server 支持

**底层存储**:
- Curvine: 高性能分布式缓存系统
- Worker 节点: 提供 Block 存储和读写接口

**启动命令**:

```bash
# 1. 启动 Curvine 集群
/home/oppo/Documents/curvine/build/dist/bin/restart-all.sh

# 2. 编译 Curvine NFS Gateway
cargo build --release -p curvine-nfs 2>&1 | tail -20
cp target/release/curvine-nfs-gateway /home/oppo/Documents/curvine/build/dist/lib

# 3. 启动 NFS Gateway
/home/oppo/Documents/curvine/build/dist/bin/curvine-nfs-gateway.sh

# 4. 挂载 NFSv4.1 文件系统
sudo umount -f /mnt/curvine-nfs
sudo mount -t nfs -o vers=4.1,port=2049,tcp,resvport 127.0.0.1:/ /mnt/curvine-nfs41
```

### 0.3 设计原则

1. **对齐 NFS-Ganesha**: 所有实现细节严格对齐 NFS-Ganesha 的参考实现
2. **基于真实证据**: 所有设计决策都基于 NFS-Ganesha 源码和 RFC 5661/5663
3. **复用现有接口**: 充分利用 Curvine Worker 已有的读写接口
4. **渐进式实现**: 先实现读取，再实现写入

## 1. 背景与目标

### 1.1 当前架构的瓶颈

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    当前 NFS Gateway 架构                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌────────┐                                                             │
│  │ Client │                                                             │
│  └───┬────┘                                                             │
│      │                                                                  │
│      │ NFS READ (所有数据经过 Gateway)                                  │
│      ▼                                                                  │
│  ┌────────────────┐                                                     │
│  │  NFS Gateway   │ ◄── 单点瓶颈                                        │
│  │  (curvine-nfs) │                                                     │
│  └───────┬────────┘                                                     │
│          │                                                              │
│          │ 串行读取各个 Block                                           │
│          ▼                                                              │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │                    Curvine Storage                            │       │
│  │  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐              │       │
│  │  │Worker 1│  │Worker 2│  │Worker 3│  │Worker 4│              │       │
│  │  │Block 0 │  │Block 1 │  │Block 2 │  │Block 3 │              │       │
│  │  └────────┘  └────────┘  └────────┘  └────────┘              │       │
│  └──────────────────────────────────────────────────────────────┘       │
│                                                                         │
│  问题:                                                                  │
│  1. Gateway 是单点瓶颈，所有数据都要经过它                              │
│  2. 无法利用 Curvine 的分布式特性                                       │
│  3. 大文件读取时，串行访问各个 Block                                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 pNFS 目标架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    pNFS 并行读取架构                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌────────┐                                                             │
│  │ Client │                                                             │
│  └───┬────┘                                                             │
│      │                                                                  │
│      │ 1. LAYOUTGET (获取文件布局)                                      │
│      ▼                                                                  │
│  ┌────────────────┐                                                     │
│  │  MDS (Gateway) │  Metadata Server - 只处理元数据                     │
│  └───────┬────────┘                                                     │
│          │                                                              │
│          │ 2. 返回 Layout (Block 位置信息)                              │
│          ▼                                                              │
│  ┌────────┐                                                             │
│  │ Client │                                                             │
│  └───┬────┘                                                             │
│      │                                                                  │
│      │ 3. 直接并行读取各个 Block                                        │
│      ├──────────────┬──────────────┬──────────────┐                     │
│      ▼              ▼              ▼              ▼                     │
│  ┌────────┐    ┌────────┐    ┌────────┐    ┌────────┐                   │
│  │Worker 1│    │Worker 2│    │Worker 3│    │Worker 4│                   │
│  │Block 0 │    │Block 1 │    │Block 2 │    │Block 3 │                   │
│  └────────┘    └────────┘    └────────┘    └────────┘                   │
│                                                                         │
│  优势:                                                                  │
│  1. Gateway 只处理元数据，不是数据瓶颈                                  │
│  2. 客户端直接并行访问多个 Worker                                       │
│  3. 吞吐量 = 所有 Worker 的总和                                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```


## 2. pNFS 协议概述

### 2.1 核心操作

| 操作 | 方向 | 说明 |
|------|------|------|
| LAYOUTGET | Client → MDS | 获取文件的数据布局 |
| LAYOUTRETURN | Client → MDS | 归还布局（释放资源） |
| LAYOUTCOMMIT | Client → MDS | 提交布局修改（写入场景） |
| CB_LAYOUTRECALL | MDS → Client | 回收布局（服务器需要修改） |

### 2.2 Layout 类型

pNFS 定义了三种 Layout 类型：

1. **LAYOUT4_NFSV4_1_FILES** - 文件级布局（我们使用这个）
2. **LAYOUT4_OSD2_OBJECTS** - 对象级布局
3. **LAYOUT4_BLOCK_VOLUME** - 块级布局

### 2.3 文件布局结构 (nfsv4_1_file_layout4)

```c
struct nfsv4_1_file_layout4 {
    deviceid4       nfl_deviceid;      // 设备 ID
    nfl_util4       nfl_util;          // 条带单元大小等
    uint32_t        nfl_first_stripe_index;  // 第一个条带索引
    offset4         nfl_pattern_offset;      // 模式偏移
    nfs_fh4         nfl_fh_list<>;     // 数据服务器文件句柄列表
};
```

## 3. Curvine 与 pNFS 的映射

### 3.1 概念映射

| pNFS 概念 | Curvine 概念 | 说明 |
|-----------|-------------|------|
| MDS (Metadata Server) | NFS Gateway | 处理元数据操作 |
| DS (Data Server) | Worker Node | 存储实际数据块 |
| Layout | Block Location | 文件块的位置信息 |
| Stripe Unit | Block Size | 128MB (curvine-block) |
| Device | Worker Cluster | Worker 节点集合 |

### 3.2 Curvine 文件结构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Curvine 文件存储结构                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  文件: /data/large_file.dat (512MB)                                     │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ Block 0 (0-128MB)     │ Block 1 (128-256MB)   │ Block 2 (256-384MB)│ │
│  │ Worker: 192.168.1.10  │ Worker: 192.168.1.11  │ Worker: 192.168.1.12│ │
│  │ Replicas: [10,11,12]  │ Replicas: [11,12,13]  │ Replicas: [12,13,10]│ │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  FileStatus.blocks = [                                                  │
│    BlockInfo { id: 0, offset: 0,   len: 128MB, workers: [10,11,12] },   │
│    BlockInfo { id: 1, offset: 128MB, len: 128MB, workers: [11,12,13] }, │
│    BlockInfo { id: 2, offset: 256MB, len: 128MB, workers: [12,13,10] }, │
│    BlockInfo { id: 3, offset: 384MB, len: 128MB, workers: [13,10,11] }, │
│  ]                                                                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```


## 4. 并行读取的顺序保证

### 4.1 问题描述

当客户端并行读取多个 Block 时，如何保证数据按正确顺序返回给应用程序？

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    并行读取的顺序问题                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  应用程序: read(fd, buf, 512MB)  // 读取 512MB                          │
│                                                                         │
│  期望顺序: Block0 → Block1 → Block2 → Block3                            │
│                                                                         │
│  并行读取:                                                              │
│  ┌────────┐    ┌────────┐    ┌────────┐    ┌────────┐                   │
│  │Block 0 │    │Block 1 │    │Block 2 │    │Block 3 │                   │
│  │ 50ms   │    │ 30ms   │    │ 80ms   │    │ 40ms   │                   │
│  └────────┘    └────────┘    └────────┘    └────────┘                   │
│                                                                         │
│  实际返回顺序: Block1(30ms) → Block3(40ms) → Block0(50ms) → Block2(80ms)│
│                                                                         │
│  问题: 如何重组为正确顺序？                                             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.2 解决方案：客户端重组

pNFS 的设计将顺序保证的责任交给客户端：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    客户端重组方案                                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                     NFS Client (Linux Kernel)                    │    │
│  ├─────────────────────────────────────────────────────────────────┤    │
│  │                                                                  │    │
│  │  1. 解析 Layout，确定每个 Block 的位置                           │    │
│  │                                                                  │    │
│  │  2. 为每个 Block 分配缓冲区（按偏移量预分配）                    │    │
│  │     ┌────────┬────────┬────────┬────────┐                        │    │
│  │     │ Buf[0] │ Buf[1] │ Buf[2] │ Buf[3] │                        │    │
│  │     │ 0-128M │128-256M│256-384M│384-512M│                        │    │
│  │     └────────┴────────┴────────┴────────┘                        │    │
│  │                                                                  │    │
│  │  3. 并行发起读取请求                                             │    │
│  │     Block0 → Worker1 → 写入 Buf[0]                               │    │
│  │     Block1 → Worker2 → 写入 Buf[1]                               │    │
│  │     Block2 → Worker3 → 写入 Buf[2]                               │    │
│  │     Block3 → Worker4 → 写入 Buf[3]                               │    │
│  │                                                                  │    │
│  │  4. 等待所有请求完成                                             │    │
│  │                                                                  │    │
│  │  5. 返回连续的缓冲区给应用程序                                   │    │
│  │                                                                  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  关键点:                                                                │
│  - 缓冲区按偏移量预分配，数据直接写入正确位置                          │
│  - 无需排序，只需等待所有请求完成                                      │
│  - Linux NFS 客户端已实现此逻辑                                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Linux pNFS 客户端实现

Linux 内核的 pNFS 客户端已经实现了并行读取和顺序重组：

```c
// fs/nfs/pnfs.c
static void pnfs_read_done(struct nfs_pgio_header *hdr)
{
    // 数据已经写入到正确的页面偏移位置
    // 无需重新排序
}

// fs/nfs/filelayout/filelayout.c
static enum pnfs_try_status
filelayout_read_pagelist(struct nfs_pgio_header *hdr)
{
    // 1. 根据 layout 确定数据服务器
    // 2. 计算正确的偏移量
    // 3. 发起并行读取
    // 4. 数据直接写入预分配的页面
}
```


## 5. Curvine pNFS 实现设计

### 5.1 架构概览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Curvine pNFS 实现架构                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                     NFS Client (Linux)                           │    │
│  └───────────────────────────┬─────────────────────────────────────┘    │
│                              │                                          │
│          ┌───────────────────┼───────────────────┐                      │
│          │                   │                   │                      │
│          ▼                   ▼                   ▼                      │
│  ┌───────────────┐   ┌───────────────┐   ┌───────────────┐              │
│  │ LAYOUTGET     │   │ READ (direct) │   │ LAYOUTRETURN  │              │
│  │ GETDEVICEINFO │   │ to Workers    │   │ LAYOUTCOMMIT  │              │
│  └───────┬───────┘   └───────┬───────┘   └───────┬───────┘              │
│          │                   │                   │                      │
│          ▼                   │                   ▼                      │
│  ┌───────────────┐           │           ┌───────────────┐              │
│  │  MDS (Gateway)│           │           │  MDS (Gateway)│              │
│  │  - Layout Mgr │           │           │  - Layout Mgr │              │
│  │  - Device Mgr │           │           │               │              │
│  └───────────────┘           │           └───────────────┘              │
│                              │                                          │
│                              ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Curvine Workers (DS)                          │    │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐              │    │
│  │  │Worker 1 │  │Worker 2 │  │Worker 3 │  │Worker 4 │              │    │
│  │  │ pNFS DS │  │ pNFS DS │  │ pNFS DS │  │ pNFS DS │              │    │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘              │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 组件设计

#### 5.2.1 Layout Manager (MDS 端)

```rust
/// Layout Manager - 管理文件布局信息
pub struct LayoutManager {
    /// 文件 ID -> Layout 状态
    layouts: RwLock<HashMap<Fileid4, LayoutState>>,
    /// Device ID -> Device Info
    devices: RwLock<HashMap<Deviceid4, DeviceInfo>>,
    /// Curvine 文件系统引用
    ufs: Arc<UnifiedFileSystem>,
}

/// Layout 状态
pub struct LayoutState {
    /// 布局类型 (LAYOUT4_NFSV4_1_FILES)
    pub layout_type: u32,
    /// 布局范围 (offset, length)
    pub range: (u64, u64),
    /// 条带单元大小 (128MB = Curvine Block Size)
    pub stripe_unit: u32,
    /// Block 信息列表
    pub blocks: Vec<BlockLayoutInfo>,
    /// 布局是否可写
    pub iomode: u32,
    /// 持有此布局的客户端
    pub clients: HashSet<Clientid4>,
}

/// Block 布局信息
pub struct BlockLayoutInfo {
    /// Block 在文件中的偏移
    pub offset: u64,
    /// Block 长度
    pub length: u64,
    /// 数据服务器地址列表 (主副本 + 备份)
    pub ds_addrs: Vec<String>,
    /// 数据服务器文件句柄
    pub ds_fh: Vec<u8>,
}
```

#### 5.2.2 Device Manager (MDS 端)

```rust
/// Device Info - 数据服务器集群信息
pub struct DeviceInfo {
    /// Device ID (16 bytes)
    pub deviceid: Deviceid4,
    /// 数据服务器地址列表
    pub ds_addrs: Vec<DsAddr>,
    /// 条带索引
    pub stripe_indices: Vec<u32>,
}

/// 数据服务器地址
pub struct DsAddr {
    /// Worker 地址 (IP:Port)
    pub addr: String,
    /// 网络类型 (tcp, tcp6)
    pub netid: String,
}
```


### 5.3 pNFS 操作实现

#### 5.3.1 LAYOUTGET 实现

**当前状态**：❌ **未实现**（handlers.rs 中返回 `Notsupp`）

**实现方案**：

```rust
/// LAYOUTGET - 获取文件布局
/// 
/// # 流程
/// 1. 从 Curvine Master 获取文件的 Block 信息（使用现有接口）
/// 2. 将 Block 信息转换为 pNFS Layout 格式
/// 3. 返回给客户端
pub async fn op_layoutget(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // 解析请求参数（RFC 5661 Section 18.43）
    let signal_layout_avail = input.read_u32::<BigEndian>()?;
    let layout_type = input.read_u32::<BigEndian>()?;  // LAYOUT4_NFSV4_1_FILES = 1
    let iomode = input.read_u32::<BigEndian>()?;       // LAYOUTIOMODE4_READ = 1
    let offset = input.read_u64::<BigEndian>()?;
    let length = input.read_u64::<BigEndian>()?;
    let minlength = input.read_u64::<BigEndian>()?;
    let mut stateid = Stateid4::default();
    stateid.deserialize(input)?;
    let maxcount = input.read_u32::<BigEndian>()?;

    // 获取文件信息
    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;
    
    // 从 Curvine 获取 Block 位置信息（使用现有接口）
    // 注意：需要实现 get_block_locations_for_pnfs 方法
    let path = handler.fs.get_path(fileid)?;
    let file_status = handler.fs.ufs().get_status(&path).await?;
    
    // 计算需要哪些 Block
    let start_block = offset / BLOCK_SIZE;
    let end_block = (offset + length + BLOCK_SIZE - 1) / BLOCK_SIZE;
    
    // 从 FileStatus.blocks 中提取相关 Block 信息
    let block_infos = extract_block_infos(
        &file_status.blocks,
        start_block,
        end_block,
        offset,
        length,
    )?;
    
    // 构建 Layout 响应
    let layout = build_file_layout(&block_infos, handler)?;
    
    // 序列化响应（RFC 5661 Section 18.43.2）
    let mut result = Vec::new();
    // ... 序列化 layout
    
    Ok(result)
}

/// 从 Curvine Block 信息构建 pNFS File Layout
fn build_file_layout(
    blocks: &[BlockLayoutInfo],
    handler: &CompoundHandler,
) -> Nfs4Result<FileLayout4> {
    let mut nfl_fh_list = Vec::new();
    
    for block in blocks {
        // 为每个 Block 创建数据服务器文件句柄
        // 文件句柄编码 Block ID（16 bytes）
        let ds_fh = encode_block_fh(block.block_id);
        nfl_fh_list.push(ds_fh);
    }
    
    // 获取或创建 Device ID
    let deviceid = handler.layout_mgr.get_or_create_deviceid()?;
    
    Ok(FileLayout4 {
        nfl_deviceid: deviceid,
        nfl_util: STRIPE_UNIT_SIZE | NFL4_UFLG_DENSE,
        nfl_first_stripe_index: 0,
        nfl_pattern_offset: 0,
        nfl_fh_list,
    })
}

/// 提取 Block 信息用于 Layout
fn extract_block_infos(
    file_blocks: &FileBlocks,
    start_block: u64,
    end_block: u64,
    offset: u64,
    length: u64,
) -> Nfs4Result<Vec<BlockLayoutInfo>> {
    let mut result = Vec::new();
    
    for (idx, block) in file_blocks.blocks.iter().enumerate() {
        let block_idx = idx as u64;
        if block_idx < start_block || block_idx >= end_block {
            continue;
        }
        
        // 计算 Block 在文件中的实际偏移和长度
        let block_file_offset = block_idx * BLOCK_SIZE;
        let block_start = offset.max(block_file_offset);
        let block_end = (offset + length).min(block_file_offset + BLOCK_SIZE);
        
        if block_start >= block_end {
            continue;
        }
        
        result.push(BlockLayoutInfo {
            block_id: block.id,
            offset: block_file_offset,
            length: block_end - block_start,
            ds_addrs: block.workers.iter().map(|w| w.to_string()).collect(),
        });
    }
    
    Ok(result)
}
```

**依赖的现有接口**：
- ✅ `handler.fs.ufs().get_status()` - 已存在
- ✅ `FileStatus.blocks` - 已存在 Block 位置信息
- ⚠️ 需要添加：`extract_block_infos` 辅助函数

#### 5.3.2 GETDEVICEINFO 实现

**当前状态**：❌ **未实现**（handlers.rs 中返回 `Notsupp`）

**实现方案**：

```rust
/// GETDEVICEINFO - 获取数据服务器信息
///
/// 客户端在收到 LAYOUTGET 响应后，需要调用此操作
/// 获取数据服务器的网络地址（RFC 5661 Section 18.40）
pub async fn op_getdeviceinfo(
    input: &mut impl Read,
    _ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut deviceid = [0u8; 16];
    input.read_exact(&mut deviceid)?;
    let layout_type = input.read_u32::<BigEndian>()?;
    let maxcount = input.read_u32::<BigEndian>()?;
    
    // 验证 layout_type
    if layout_type != LAYOUT4_NFSV4_1_FILES {
        return Err(Nfs4Status::Notsupp.into());
    }
    
    // 获取所有 Worker 节点地址（从 Curvine Master）
    // 注意：需要实现 get_worker_addresses 方法
    let workers = handler.layout_mgr.get_worker_addresses().await?;
    
    // 构建 Device Info（RFC 5661 Section 18.40.2）
    let device_info = DeviceInfo {
        deviceid,
        layout_type,
        da_addr_body: build_multipath_list(&workers)?,
    };
    
    // 序列化响应
    let mut result = Vec::new();
    device_info.serialize(&mut result)?;
    
    Ok(result)
}

/// 构建多路径地址列表（RFC 5663 Section 4.1）
fn build_multipath_list(workers: &[WorkerAddress]) -> Nfs4Result<Vec<u8>> {
    let mut result = Vec::new();
    
    // stripe_indices 数量
    (workers.len() as u32).serialize(&mut result)?;
    
    for (idx, worker) in workers.iter().enumerate() {
        // stripe_index
        (idx as u32).serialize(&mut result)?;
        
        // multipath_list (每个 stripe 可以有多个地址用于容错)
        // 当前实现：每个 Worker 一个地址
        // 未来可以扩展：支持多个网络接口（主备）
        1u32.serialize(&mut result)?;  // 地址数量
        
        // netaddr4 (RFC 5661 Section 4.2)
        let netid = b"tcp".to_vec();  // 或 "tcp6" for IPv6
        netid.serialize(&mut result)?;
        
        // 地址格式: "IP.IP.IP.IP.PORT_HI.PORT_LO" (RFC 5661 Section 4.2)
        // 注意: 所有 DS 使用标准端口 2049（与 MDS 相同）
        // 例如: "192.168.1.10" → "192.168.1.10.0.80" (端口 2049)
        let addr = format_nfs_addr(&worker.addr)?;
        addr.serialize(&mut result)?;
    }
    
    Ok(result)
}

/// 格式化 Worker 地址为 NFS netaddr4 格式
fn format_nfs_addr(worker_addr: &WorkerAddress) -> Nfs4Result<Vec<u8>> {
    // WorkerAddress 格式: "IP:Port" 或 "IP"
    // pNFS DS 使用标准端口 2049（与 MDS 相同）
    // 例如: "192.168.1.10" → [192, 168, 1, 10, 0, 80]
    // Port 2049 = 0x0801 → [0x08, 0x01] → [0, 80] (big-endian)
    
    // 实现细节：
    // 1. 解析 IP 地址
    // 2. 端口固定为 2049（标准 NFS 端口）
    // 3. 转换为 6 字节格式: [IP1, IP2, IP3, IP4, PORT_HI, PORT_LO]
    // ...
}
```

**依赖的现有接口**：
- ⚠️ 需要添加：`get_worker_addresses()` - 从 Master 获取 Worker 地址列表
- ✅ Worker 地址信息：Worker 注册时包含地址信息


### 5.4 数据服务器 (DS) 实现

#### 5.4.1 Worker 端架构分析

**当前状态**：

Curvine Worker 已经存在读写接口，但这是 **Curvine 内部的 RPC 协议**（基于 protobuf），不是标准的 NFS 协议：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Curvine Worker 当前架构                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                     Curvine Worker                               │    │
│  ├─────────────────────────────────────────────────────────────────┤    │
│  │                                                                  │    │
│  │  ┌─────────────────┐    ┌─────────────────┐                      │    │
│  │  │  Block Storage  │◄───│  Curvine RPC    │◄─── Curvine Client  │    │
│  │  │  (existing)     │    │  (protobuf)     │   (Gateway/FUSE)    │    │
│  │  │                 │    │                 │                      │    │
│  │  │                 │    │  - BlockReadRequest                   │    │
│  │  │                 │    │  - BlockWriteRequest                  │    │
│  │  └─────────────────┘    └─────────────────┘                      │    │
│  │                                                                  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  问题: NFS Client 无法直接使用 Curvine RPC                              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**pNFS DS 服务需求**：

pNFS 客户端需要**标准的 NFSv4.1 协议**来直接访问 Worker，因此需要实现一个 NFS DS 服务层：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Worker pNFS DS 服务架构                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                     Curvine Worker                               │    │
│  ├─────────────────────────────────────────────────────────────────┤    │
│  │                                                                  │    │
│  │  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐ │    │
│  │  │  Block Storage  │◄───│  Curvine RPC    │◄───│  pNFS DS     │ │    │
│  │  │  (existing)     │    │  (existing)     │    │  Service     │ │    │
│  │  │                 │    │                 │    │  (new)       │ │    │
│  │  └─────────────────┘    └─────────────────┘    └──────┬───────┘ │    │
│  │                                                         │         │    │
│  │                                                         │ NFSv4.1 │    │
│  │                                                         ▼         │    │
│  │                                                  ┌──────────────┐ │    │
│  │                                                  │ NFS Client   │ │    │
│  │                                                  │ (Linux Kernel)│ │    │
│  │                                                  └──────────────┘ │    │
│  │                                                                  │    │
│  │  pNFS DS 服务需要实现:                                          │    │
│  │  - NFSv4.1 READ: 标准 NFS 协议，内部调用 Curvine RPC            │    │
│  │  - NFSv4.1 WRITE: 标准 NFS 协议，内部调用 Curvine RPC (可选)    │    │
│  │  - NFSv4.1 COMMIT: 标准 NFS 协议 (可选)                         │    │
│  │                                                                  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 5.4.2 Worker 现有接口分析

**Curvine RPC 接口**（已存在）：

```protobuf
// curvine-common/proto/worker.proto

message BlockReadRequest {
    required int64 id = 1;           // Block ID
    required int64 off = 2;          // Block 内偏移
    required int64 len = 3;          // 读取长度
    required int32 chunk_size = 4;   // 块大小
    required bool short_circuit = 5;  // 是否短路读取
    required bool enable_read_ahead = 8;
    required int64 read_ahead_len = 9;
    required int64 drop_cache_len = 10;
}

message BlockWriteRequest {
    required ExtendedBlockProto block = 1;
    required int64 off = 2;
    required int64 block_size = 3;
    required bool short_circuit = 4;
    required string client_name = 5;
    required int32 chunk_size = 6;
}
```

**接口评估**：

✅ **满足 pNFS DS 需求**：
- Block ID 标识：`BlockReadRequest.id` 可以标识 Block
- 偏移和长度：`off` 和 `len` 支持部分读取
- 读取接口：`ReadHandler` 已实现完整的读取逻辑

⚠️ **需要适配**：
- **协议转换**：需要将 NFSv4.1 READ 请求转换为 `BlockReadRequest`
- **文件句柄解析**：NFS 文件句柄需要编码 Block ID 信息
- **NFS 协议实现**：需要实现标准的 NFSv4.1 READ/WRITE 操作

#### 5.4.3 pNFS DS 服务实现方案

**端口配置**：✅ **复用标准 NFS 端口 2049**

pNFS DS 服务**不需要单独端口**，可以复用标准 NFS 端口 2049：

- **标准做法**：所有 NFS 服务（MDS 和 DS）都使用端口 2049
- **客户端识别**：客户端通过 GETDEVICEINFO 返回的 IP 地址区分不同的 DS
- **部署优势**：
  - 符合 NFS 标准（RFC 5661）
  - 无需防火墙特殊配置
  - 客户端无需特殊端口配置
  - 如果 Worker 和 Gateway 在同一台机器，可通过不同 IP 地址区分

**方案一：独立 NFS DS 服务**（推荐）

在每个 Worker 上运行一个轻量级的 NFS DS 服务，实现标准 NFSv4.1 协议：

```rust
/// pNFS DS 服务 - 桥接 NFS 协议和 Curvine RPC
pub struct PnfsDataServer {
    /// Curvine Worker RPC 客户端
    curvine_client: Arc<CurvineRpcClient>,
    /// Block Store（用于直接访问，可选优化）
    block_store: Option<Arc<BlockStore>>,
}

impl PnfsDataServer {
    /// 启动 pNFS DS 服务
    /// 
    /// 监听标准 NFS 端口 2049（与 Gateway 相同）
    /// 客户端通过 IP 地址区分 MDS 和 DS
    pub async fn start(&self, listen_addr: &str) -> Result<(), Error> {
        // 复用 curvine-nfs 的 NFS 服务器代码
        // 监听端口 2049（标准 NFS 端口）
        let listener = TcpListener::bind(format!("{}:2049", listen_addr)).await?;
        // ...
    }
}

impl PnfsDataServer {
    /// NFSv4.1 READ 操作
    /// 
    /// 1. 解析文件句柄获取 Block ID
    /// 2. 调用 Curvine RPC 读取数据
    /// 3. 返回标准 NFS 响应
    pub async fn nfs_read(
        &self,
        fh: &[u8],      // NFS 文件句柄（编码 Block ID）
        offset: u64,   // 文件偏移（需要转换为 Block 内偏移）
        count: u32,
    ) -> Nfs4Result<Vec<u8>> {
        // 1. 解析文件句柄
        let block_id = parse_block_id_from_fh(fh)?;
        let block_offset = offset % BLOCK_SIZE;
        
        // 2. 调用 Curvine RPC
        let req = BlockReadRequest {
            id: block_id,
            off: block_offset,
            len: count as i64,
            chunk_size: DEFAULT_CHUNK_SIZE,
            short_circuit: false,
            enable_read_ahead: true,
            read_ahead_len: 4 * 1024 * 1024,
            drop_cache_len: 1024 * 1024,
        };
        
        let data = self.curvine_client.read_block(req).await?;
        
        // 3. 返回 NFS 格式数据
        Ok(data)
    }
    
    /// NFSv4.1 WRITE 操作（可选）
    pub async fn nfs_write(
        &self,
        fh: &[u8],
        offset: u64,
        data: &[u8],
        stable: bool,
    ) -> Nfs4Result<WriteResult> {
        // 类似 READ，调用 Curvine RPC
        // ...
    }
}
```

**文件句柄编码方案**：

```rust
/// 编码 Block ID 到 NFS 文件句柄
/// 
/// 文件句柄格式（16 bytes）:
/// [0-7]: Block ID (u64, big-endian)
/// [8-15]: 保留（可用于 Block 偏移或其他元数据）
fn encode_block_fh(block_id: u64) -> Vec<u8> {
    let mut fh = vec![0u8; 16];
    fh[0..8].copy_from_slice(&block_id.to_be_bytes());
    fh
}

/// 从 NFS 文件句柄解析 Block ID
fn parse_block_id_from_fh(fh: &[u8]) -> Nfs4Result<u64> {
    if fh.len() < 8 {
        return Err(Nfs4Status::BadHandle.into());
    }
    let block_id = u64::from_be_bytes([
        fh[0], fh[1], fh[2], fh[3],
        fh[4], fh[5], fh[6], fh[7],
    ]);
    Ok(block_id)
}
```

**方案二：复用现有 NFS Gateway 代码**（简化实现，推荐）

如果 Worker 和 Gateway 在同一代码库，可以**完全复用** NFS Gateway 的代码：

```rust
// Worker 端启动 pNFS DS 服务
pub async fn start_pnfs_ds_service(worker: &Worker) -> Result<(), Error> {
    // 复用 curvine-nfs 的 NFS 服务器代码
    // 使用标准端口 2049（与 Gateway 相同）
    let ds_fs = PnfsDsFileSystem::new(worker.block_store.clone());
    
    // 复用相同的 NfsServer 实现
    let nfs_server = NfsServer::new(ds_fs);
    
    // 监听标准 NFS 端口 2049
    // 注意：如果 Worker 和 Gateway 在同一台机器，需要：
    // 1. 使用不同的 IP 地址（推荐）
    // 2. 或者使用 SO_REUSEPORT（Linux 3.9+）
    nfs_server.listen("0.0.0.0:2049").await?;
    
    Ok(())
}
```

**端口复用说明**：

1. **标准做法**：MDS 和 DS 都使用端口 2049
   - Gateway (MDS): `192.168.1.100:2049`
   - Worker 1 (DS): `192.168.1.10:2049`
   - Worker 2 (DS): `192.168.1.11:2049`
   - Worker 3 (DS): `192.168.1.12:2049`

2. **客户端识别**：
   - 客户端通过 GETDEVICEINFO 返回的 IP 地址连接不同的 DS
   - 所有连接都使用标准端口 2049
   - 无需特殊端口配置

3. **同机部署**（Worker 和 Gateway 在同一台机器）：
   - **方案 A**：使用不同 IP 地址（推荐）
     - Gateway: `192.168.1.100:2049`
     - DS: `192.168.1.101:2049`（绑定到另一个 IP）
   - **方案 B**：使用 SO_REUSEPORT（Linux 3.9+）
     - 多个进程可以绑定同一端口
     - 内核自动负载均衡连接

4. **优势**：
   - ✅ 符合 NFS 标准（RFC 5661）
   - ✅ 简化客户端配置（无需特殊端口）
   - ✅ 简化防火墙规则（只需开放 2049）
   - ✅ 代码复用（DS 服务可以复用 Gateway 的 NFS 协议代码）

#### 5.4.4 实现状态

**已具备的基础**：
- ✅ Worker 有 `BlockReadRequest`/`BlockReadResponse` RPC 接口
- ✅ Worker 有 `BlockWriteRequest`/`BlockWriteResponse` RPC 接口
- ✅ Worker 有 `ReadHandler` 和 `WriteHandler` 实现
- ✅ Gateway 有 `get_block_locations` 接口（可用于 LAYOUTGET）

**待实现**：
- ❌ pNFS DS 服务：标准 NFSv4.1 协议实现
- ❌ 文件句柄编码：Block ID → NFS 文件句柄
- ❌ 协议桥接：NFS READ → Curvine RPC
- ❌ MDS 端 LAYOUTGET：返回包含 Block 信息的 Layout
- ❌ MDS 端 GETDEVICEINFO：返回 Worker 地址信息

### 5.5 完整读取流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    pNFS 并行读取完整流程                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  时间线 ──────────────────────────────────────────────────────────►     │
│                                                                         │
│  Client                    MDS (Gateway)              Workers           │
│    │                           │                         │              │
│    │  1. OPEN                  │                         │              │
│    │ ─────────────────────────►│                         │              │
│    │                           │                         │              │
│    │  2. LAYOUTGET             │                         │              │
│    │ ─────────────────────────►│                         │              │
│    │                           │                         │              │
│    │                           │ 查询 Block 位置         │              │
│    │                           │ ───────────────────────►│              │
│    │                           │                         │              │
│    │  3. Layout Response       │                         │              │
│    │ ◄─────────────────────────│                         │              │
│    │  (Block0@W1, Block1@W2,   │                         │              │
│    │   Block2@W3, Block3@W4)   │                         │              │
│    │                           │                         │              │
│    │  4. GETDEVICEINFO         │                         │              │
│    │ ─────────────────────────►│                         │              │
│    │                           │                         │              │
│    │  5. Device Info Response  │                         │              │
│    │ ◄─────────────────────────│                         │              │
│    │  (W1=192.168.1.10,        │                         │              │
│    │   W2=192.168.1.11,        │                         │              │
│    │   ...)                    │                         │              │
│    │  注意: 所有 DS 使用标准端口 2049                      │              │
│    │                           │                         │              │
│    │  6. 并行 READ (直接到 Workers)                      │              │
│    │ ─────────────────────────────────────────────────►  │ W1           │
│    │ ─────────────────────────────────────────────────►  │ W2           │
│    │ ─────────────────────────────────────────────────►  │ W3           │
│    │ ─────────────────────────────────────────────────►  │ W4           │
│    │                           │                         │              │
│    │  7. 并行返回数据                                    │              │
│    │ ◄─────────────────────────────────────────────────  │              │
│    │                           │                         │              │
│    │  8. 客户端重组数据 (按偏移量)                       │              │
│    │                           │                         │              │
│    │  9. LAYOUTRETURN (可选)   │                         │              │
│    │ ─────────────────────────►│                         │              │
│    │                           │                         │              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```


## 6. 性能预估

### 6.1 理论吞吐量对比

| 场景 | 当前架构 | pNFS 架构 | 提升倍数 |
|------|---------|----------|---------|
| 单文件读取 (4 Workers) | 1 GB/s | 4 GB/s | 4x |
| 单文件读取 (8 Workers) | 1 GB/s | 8 GB/s | 8x |
| 多客户端并发 | 受 Gateway 限制 | 线性扩展 | N x |

### 6.2 延迟分析

```
当前架构延迟:
  Client → Gateway → Worker → Gateway → Client
  RTT = 2 * (Client-Gateway) + 2 * (Gateway-Worker)
  
pNFS 架构延迟:
  Client → Worker → Client (数据路径)
  Client → Gateway → Client (元数据路径，仅一次)
  RTT = 2 * (Client-Worker)  // 数据传输
  
延迟减少: 约 50% (消除 Gateway 中转)
```

## 7. 实现计划

### 7.1 阶段一：基础 pNFS 支持 (2 周)

1. **MDS 端实现**
   - [ ] LAYOUTGET 操作
     - [ ] 实现 `op_layoutget` 处理函数
     - [ ] 从 `FileStatus.blocks` 提取 Block 信息
     - [ ] 构建 pNFS File Layout 响应
     - [ ] 文件句柄编码（Block ID → NFS FH）
   - [ ] GETDEVICEINFO 操作
     - [ ] 实现 `op_getdeviceinfo` 处理函数
     - [ ] 从 Master 获取 Worker 地址列表
     - [ ] 构建 Device Info 响应（multipath_list）
   - [ ] LAYOUTRETURN 操作
     - [ ] 实现 `op_layoutreturn` 处理函数
     - [ ] 清理 Layout 状态
   - [ ] Layout Manager
     - [ ] Layout 状态管理
     - [ ] Device ID 管理

2. **协议支持**
   - [ ] pNFS File Layout 编解码（RFC 5663）
   - [ ] Device Info 编解码（RFC 5661）
   - [ ] 文件句柄编解码（Block ID ↔ NFS FH）

### 7.2 阶段二：DS 服务 (2-3 周)

1. **Worker 端实现**
   - [ ] pNFS DS 服务框架
     - [ ] NFSv4.1 协议服务器（复用 curvine-nfs 代码）
     - [ ] 监听标准 NFS 端口 2049（与 Gateway 相同，通过 IP 区分）
     - [ ] 最小化 NFS 操作集（READ/WRITE/COMMIT/GETATTR）
     - [ ] 文件句柄处理（Block ID 编码/解码）
   - [ ] DS READ 操作
     - [ ] 解析 NFS 文件句柄获取 Block ID
     - [ ] 调用 Curvine RPC `BlockReadRequest`
     - [ ] 返回标准 NFS READ 响应
   - [ ] DS WRITE 操作（可选，初期可跳过）
     - [ ] 解析文件句柄
     - [ ] 调用 Curvine RPC `BlockWriteRequest`
     - [ ] 返回标准 NFS WRITE 响应
   - [ ] 文件句柄处理
     - [ ] Block ID 编码到 NFS 文件句柄
     - [ ] 从 NFS 文件句柄解析 Block ID

2. **测试**
   - [ ] 单 Worker 读取测试
   - [ ] 多 Worker 并行读取测试
   - [ ] 与 Linux pNFS 客户端兼容性测试

### 7.3 阶段三：优化与完善 (1 周)

1. **性能优化**
   - [ ] Layout 缓存
   - [ ] 连接池
   - [ ] 预读取

2. **容错处理**
   - [ ] Worker 故障切换
   - [ ] Layout 失效处理

## 8. 风险与挑战

### 8.1 技术风险

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Linux pNFS 客户端兼容性 | 高 | 严格遵循 RFC 5661/5663 |
| Worker 直接访问安全性 | 中 | 使用 AUTH_SYS 或 RPCSEC_GSS |
| Layout 一致性 | 中 | 实现 CB_LAYOUTRECALL |

### 8.2 已知限制

1. **写入支持**：初期只支持读取，写入仍走 Gateway
2. **小文件**：小于一个 Block 的文件不受益于 pNFS
3. **客户端要求**：需要 Linux 内核 pNFS 支持 (2.6.37+)
4. **DS 服务部署**：需要在每个 Worker 上部署 pNFS DS 服务（复用端口 2049）
5. **协议转换开销**：NFS → Curvine RPC 的转换会有少量开销
6. **端口复用**：如果 Worker 和 Gateway 在同一台机器，需要使用不同 IP 或 SO_REUSEPORT

### 8.3 实现状态总结

**已具备的基础**：
- ✅ Curvine Worker 有完整的 Block 读写 RPC 接口
- ✅ Gateway 有 `get_block_locations` 接口（可用于 LAYOUTGET）
- ✅ FileStatus.blocks 包含 Block 位置信息
- ✅ Worker 地址信息可从 Master 获取

**待实现的核心功能**：
- ❌ MDS 端：LAYOUTGET、GETDEVICEINFO、LAYOUTRETURN
- ❌ DS 端：NFSv4.1 协议服务器、READ/WRITE 操作
- ❌ 协议桥接：NFS 协议 ↔ Curvine RPC
- ❌ 文件句柄编码：Block ID ↔ NFS 文件句柄

**技术难点**：
1. **文件句柄设计**：需要设计紧凑且可扩展的文件句柄格式
2. **协议兼容性**：确保与 Linux pNFS 客户端完全兼容
3. **错误处理**：Worker 故障时的 Layout 失效和恢复
4. **性能优化**：减少协议转换开销

## 9. 实现状态与依赖分析

### 9.1 当前实现状态

| 组件 | 状态 | 说明 |
|------|------|------|
| **MDS 端 (Gateway)** | | |
| LAYOUTGET | ❌ 未实现 | handlers.rs 返回 `Notsupp` |
| GETDEVICEINFO | ❌ 未实现 | handlers.rs 返回 `Notsupp` |
| LAYOUTRETURN | ❌ 未实现 | handlers.rs 返回 `Notsupp` |
| LAYOUTCOMMIT | ❌ 未实现 | handlers.rs 返回 `Notsupp` |
| Layout Manager | ❌ 未实现 | 需要新建模块 |
| Device Manager | ❌ 未实现 | 需要新建模块 |
| **DS 端 (Worker)** | | |
| NFS DS 服务 | ❌ 未实现 | 需要新建服务 |
| DS READ 操作 | ❌ 未实现 | 需要实现 NFS 协议 |
| DS WRITE 操作 | ❌ 未实现 | 可选，初期可跳过 |
| **基础接口** | | |
| get_block_locations | ✅ 已存在 | `curvine_nfs_fs.rs` |
| FileStatus.blocks | ✅ 已存在 | Curvine 文件状态 |
| Worker RPC 接口 | ✅ 已存在 | BlockReadRequest/BlockWriteRequest |
| Worker 地址信息 | ✅ 已存在 | Worker 注册时包含 |

### 9.2 依赖关系分析

```
实现 pNFS 的依赖链：

LAYOUTGET
  ├─ get_block_locations() ✅ 已存在
  ├─ FileStatus.blocks ✅ 已存在
  └─ Layout Manager ❌ 待实现
      └─ Device Manager ❌ 待实现

GETDEVICEINFO
  ├─ Worker 地址列表 ⚠️ 需要从 Master 获取
  └─ Device Manager ❌ 待实现

DS READ
  ├─ NFS DS 服务 ❌ 待实现
  ├─ 文件句柄解析 ❌ 待实现
  └─ Curvine RPC ✅ 已存在
      └─ BlockReadRequest ✅ 已存在
```

### 9.3 Worker 接口满足度评估

**BlockReadRequest 接口分析**：

| pNFS DS 需求 | Curvine RPC | 满足度 | 说明 |
|-------------|------------|--------|------|
| Block 标识 | `id: int64` | ✅ 完全满足 | Block ID 唯一标识 |
| 偏移量 | `off: int64` | ✅ 完全满足 | Block 内偏移 |
| 读取长度 | `len: int64` | ✅ 完全满足 | 支持部分读取 |
| 块大小 | `chunk_size: int32` | ✅ 完全满足 | 可配置 |
| 性能优化 | `enable_read_ahead` | ✅ 完全满足 | 预读支持 |
| 协议 | protobuf RPC | ⚠️ 需要桥接 | 不是 NFS 协议 |

**结论**：Worker 的 RPC 接口**完全满足** pNFS DS 的功能需求，但需要额外的 NFS 协议层来桥接。

### 9.4 实现建议

**优先级排序**：

1. **P0（核心功能）**：
   - MDS 端 LAYOUTGET（复用现有 `get_block_locations`）
   - MDS 端 GETDEVICEINFO（从 Master 获取 Worker 地址）
   - DS 端 NFS READ（桥接到 Curvine RPC）

2. **P1（完整功能）**：
   - MDS 端 LAYOUTRETURN
   - 文件句柄编码/解码
   - Layout Manager

3. **P2（优化功能）**：
   - DS 端 NFS WRITE
   - MDS 端 LAYOUTCOMMIT
   - Layout 缓存优化

**实现策略**：

1. **复用现有代码**：
   - 复用 `curvine-nfs` 的 NFS 协议处理代码（用于 DS 服务）
   - 复用 `get_block_locations` 逻辑（用于 LAYOUTGET）
   - 复用 Worker RPC 客户端（用于 DS READ）

2. **最小化实现**：
   - DS 服务只需实现 READ 操作（初期）
   - Layout Manager 可以简化（无持久化）
   - Device Manager 可以静态配置（从配置文件读取 Worker 地址）

## 10. 参考资料

- RFC 5661: NFSv4.1 Protocol
- RFC 5663: pNFS File Layout
- Linux Kernel pNFS: `fs/nfs/pnfs.c`, `fs/nfs/filelayout/`
- NFS-Ganesha pNFS: `src/FSAL/FSAL_GPFS/fsal_pnfs.c`
- NFS-Ganesha DS: `src/support/ds.c`

## 11. 附录：XDR 定义

```xdr
/* Layout Types */
const LAYOUT4_NFSV4_1_FILES = 1;

/* Layout IOMode */
const LAYOUTIOMODE4_READ = 1;
const LAYOUTIOMODE4_RW   = 2;

/* File Layout */
struct nfsv4_1_file_layout4 {
    deviceid4       nfl_deviceid;
    nfl_util4       nfl_util;
    uint32_t        nfl_first_stripe_index;
    offset4         nfl_pattern_offset;
    nfs_fh4         nfl_fh_list<>;
};

/* Device Address */
struct da_addr_body {
    uint32_t        stripe_indices<>;
    multipath_list4 multipath_ds_list<>;
};
```

---

**文档版本**: 1.1  
**创建日期**: 2025-12-31  
**更新日期**: 2025-01-01  
**最后更新**: 审查并更新 DS 服务实现方案，明确 Worker 接口满足度

**参考实现**: 
- NFS-Ganesha: `/home/oppo/Documents/nfs-ganesha/src/Protocols/NFS`
- 核心文件:
  - `src/FSAL/FSAL_GPFS/fsal_pnfs.c`: pNFS 布局管理
  - `src/support/ds.c`: Data Server 支持
  - `src/include/pnfs_utils.h`: pNFS 工具函数

**开发环境**:
- Curvine 集群: `/home/oppo/Documents/curvine/build/dist/bin/restart-all.sh`
- NFS Gateway: `/home/oppo/Documents/curvine/build/dist/bin/curvine-nfs-gateway.sh`
- 挂载点: `/mnt/curvine-nfs41` (NFSv4.1)

**关键发现**:
- ✅ Worker 已有完整的 Block 读写 RPC 接口（`BlockReadRequest`/`BlockWriteRequest`）
- ⚠️ Worker 接口是 Curvine 内部 RPC，不是标准 NFS 协议
- ❌ 需要实现 pNFS DS 服务层，桥接 NFS 协议和 Curvine RPC
