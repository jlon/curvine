# pNFS 并行读取设计文档

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

```rust
/// LAYOUTGET - 获取文件布局
/// 
/// # 流程
/// 1. 从 Curvine Master 获取文件的 Block 信息
/// 2. 将 Block 信息转换为 pNFS Layout 格式
/// 3. 返回给客户端
pub async fn op_layoutget(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // 解析请求参数
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
    
    // 从 Curvine 获取 Block 位置信息
    let block_infos = handler.fs.get_block_locations(fileid, offset, length).await?;
    
    // 构建 Layout 响应
    let layout = build_file_layout(&block_infos, handler)?;
    
    // 序列化响应
    let mut result = Vec::new();
    // ... 序列化 layout
    
    Ok(result)
}

/// 从 Curvine Block 信息构建 pNFS File Layout
fn build_file_layout(
    blocks: &[CurvineBlockInfo],
    handler: &CompoundHandler,
) -> Nfs4Result<FileLayout4> {
    let mut nfl_fh_list = Vec::new();
    
    for block in blocks {
        // 为每个 Block 创建数据服务器文件句柄
        // 句柄包含: block_id + worker_addr
        let ds_fh = create_ds_filehandle(block)?;
        nfl_fh_list.push(ds_fh);
    }
    
    Ok(FileLayout4 {
        nfl_deviceid: handler.layout_mgr.get_deviceid(),
        nfl_util: STRIPE_UNIT_SIZE | NFL4_UFLG_DENSE,
        nfl_first_stripe_index: 0,
        nfl_pattern_offset: 0,
        nfl_fh_list,
    })
}
```

#### 5.3.2 GETDEVICEINFO 实现

```rust
/// GETDEVICEINFO - 获取数据服务器信息
///
/// 客户端在收到 LAYOUTGET 响应后，需要调用此操作
/// 获取数据服务器的网络地址
pub async fn op_getdeviceinfo(
    input: &mut impl Read,
    _ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut deviceid = [0u8; 16];
    input.read_exact(&mut deviceid)?;
    let layout_type = input.read_u32::<BigEndian>()?;
    let maxcount = input.read_u32::<BigEndian>()?;
    
    // 获取所有 Worker 节点地址
    let workers = handler.layout_mgr.get_workers().await?;
    
    // 构建 Device Info
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

/// 构建多路径地址列表
fn build_multipath_list(workers: &[WorkerInfo]) -> Nfs4Result<Vec<u8>> {
    let mut result = Vec::new();
    
    // stripe_indices 数量
    (workers.len() as u32).serialize(&mut result)?;
    
    for (idx, worker) in workers.iter().enumerate() {
        // stripe_index
        (idx as u32).serialize(&mut result)?;
        
        // multipath_list (每个 stripe 可以有多个地址用于容错)
        1u32.serialize(&mut result)?;  // 地址数量
        
        // netaddr4
        let netid = b"tcp".to_vec();
        netid.serialize(&mut result)?;
        
        // 地址格式: "IP.IP.IP.IP.PORT_HI.PORT_LO"
        let addr = format_nfs_addr(&worker.addr)?;
        addr.serialize(&mut result)?;
    }
    
    Ok(result)
}
```


### 5.4 数据服务器 (DS) 实现

#### 5.4.1 Worker 端 pNFS 服务

每个 Curvine Worker 需要运行一个轻量级的 pNFS DS 服务：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Worker pNFS DS 服务                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                     Curvine Worker                               │    │
│  ├─────────────────────────────────────────────────────────────────┤    │
│  │                                                                  │    │
│  │  ┌─────────────────┐    ┌─────────────────┐                      │    │
│  │  │  Block Storage  │    │   pNFS DS       │                      │    │
│  │  │  (existing)     │◄───│   Service       │◄─── NFS Client       │    │
│  │  │                 │    │   (new)         │                      │    │
│  │  └─────────────────┘    └─────────────────┘                      │    │
│  │                                                                  │    │
│  │  pNFS DS 只需实现:                                               │    │
│  │  - READ: 读取指定 Block 的数据                                   │    │
│  │  - WRITE: 写入指定 Block 的数据 (可选)                           │    │
│  │  - COMMIT: 提交写入 (可选)                                       │    │
│  │                                                                  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 5.4.2 DS READ 实现

```rust
/// pNFS DS READ 操作
/// 
/// 客户端直接向 Worker 发送 READ 请求
/// 文件句柄包含 Block ID 信息
pub async fn ds_read(
    block_id: u64,
    offset: u64,      // Block 内偏移
    count: u32,
    storage: &BlockStorage,
) -> Result<Vec<u8>, Error> {
    // 直接从本地存储读取
    let data = storage.read_block(block_id, offset, count).await?;
    Ok(data)
}
```

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
│    │  (W1=192.168.1.10:2049,   │                         │              │
│    │   W2=192.168.1.11:2049,   │                         │              │
│    │   ...)                    │                         │              │
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
   - [ ] GETDEVICEINFO 操作
   - [ ] LAYOUTRETURN 操作
   - [ ] Layout Manager

2. **协议支持**
   - [ ] pNFS File Layout 编解码
   - [ ] Device Info 编解码

### 7.2 阶段二：DS 服务 (2 周)

1. **Worker 端实现**
   - [ ] pNFS DS 服务框架
   - [ ] DS READ 操作
   - [ ] Block 直接访问接口

2. **测试**
   - [ ] 单 Worker 读取测试
   - [ ] 多 Worker 并行读取测试

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

## 9. 参考资料

- RFC 5661: NFSv4.1 Protocol
- RFC 5663: pNFS File Layout
- Linux Kernel pNFS: fs/nfs/pnfs.c, fs/nfs/filelayout/
- NFS-Ganesha pNFS: src/FSAL/FSAL_GPFS/fsal_pnfs.c

## 10. 附录：XDR 定义

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
