# Curvine NFS Delegations 设计文档

## 1. 概述

### 1.1 什么是 Delegation？

Delegation（委托）是 NFSv4 的核心特性，允许服务器将文件的部分控制权"委托"给客户端，使客户端可以在本地缓存文件数据和属性，而无需每次都与服务器通信。

```
场景：编译大型项目 (make)

Without Delegation:
  make: stat(Makefile) → Server: GETATTR → 10ms
  make: stat(Makefile) → Server: GETATTR → 10ms
  make: stat(Makefile) → Server: GETATTR → 10ms
  ... (重复 1000 次)
  
  总延迟: 1000 * 10ms = 10 秒！

With Delegation:
  make: OPEN(Makefile, WANT_READ_DELEG)
  Server: "给你 Read Delegation，你可以缓存这个文件的属性"
  make: stat(Makefile) → 本地缓存 → 0ms
  make: stat(Makefile) → 本地缓存 → 0ms
  ... (重复 1000 次)
  
  总延迟: 1 * 10ms = 10ms！
```

### 1.2 NFS-Ganesha 参考

本设计对齐 NFS-Ganesha 的实现：
- `src/SAL/state_deleg.c`: Delegation 状态管理
- `src/Protocols/NFS/nfs4_op_delegreturn.c`: DELEGRETURN 操作
- `src/MainNFSD/nfs_rpc_callback.c`: Backchannel 回调

## 2. Delegation 类型

### 2.1 Read Delegation (OPEN_DELEGATE_READ)

```
授予条件：
- 文件没有其他客户端持有 Write Delegation
- 文件没有其他客户端正在写入

客户端权限：
- 可以缓存文件数据和属性
- 可以本地执行 READ 操作
- 不能本地执行 WRITE 操作

回收条件：
- 任何客户端请求写入该文件
```

### 2.2 Write Delegation (OPEN_DELEGATE_WRITE)

```
授予条件：
- 文件没有任何其他客户端打开
- 文件没有任何其他 Delegation

客户端权限：
- 可以缓存文件数据和属性
- 可以本地执行 READ 和 WRITE 操作
- 可以延迟将写入数据发送到服务器

回收条件：
- 任何其他客户端请求访问该文件
```

## 3. 架构设计

### 3.1 组件关系

```
┌─────────────────────────────────────────────────────────────────┐
│                    DelegationManager                             │
├─────────────────────────────────────────────────────────────────┤
│  delegations: HashMap<Fileid, Delegation>                       │
│  client_delegations: HashMap<Clientid, Vec<Fileid>>             │
│  stateid_to_file: HashMap<Stateid, Fileid>                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ recall()
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BackchannelManager                            │
├─────────────────────────────────────────────────────────────────┤
│  client_channels: HashMap<Clientid, BackchannelConn>            │
│  recall_delegation(clientid, stateid, fh, truncate)             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ CB_COMPOUND [CB_SEQUENCE, CB_RECALL]
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    NFS Client                                    │
├─────────────────────────────────────────────────────────────────┤
│  收到 CB_RECALL 后：                                             │
│  1. 刷新本地缓存的写入数据                                        │
│  2. 发送 DELEGRETURN 归还 delegation                             │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 状态机

```
                    ┌─────────────┐
                    │   NONE      │
                    └──────┬──────┘
                           │ OPEN(WANT_DELEG)
                           │ + should_we_grant_deleg() = true
                           ▼
                    ┌─────────────┐
                    │  GRANTED    │
                    └──────┬──────┘
                           │ conflict detected
                           │ (another client access)
                           ▼
                    ┌─────────────┐
                    │  RECALLING  │──────────────┐
                    └──────┬──────┘              │
                           │ CB_RECALL success   │ timeout
                           │ + DELEGRETURN       │
                           ▼                     ▼
                    ┌─────────────┐       ┌─────────────┐
                    │  RETURNED   │       │  REVOKED    │
                    └─────────────┘       └─────────────┘
```

## 4. 核心流程

### 4.1 授予 Delegation

```
Client                          Server
   │                              │
   │── OPEN(file, WANT_DELEG) ───>│
   │                              │
   │                              │ 1. can_we_grant_deleg()?
   │                              │    - 检查是否有冲突的 delegation
   │                              │    - 检查是否有冲突的锁
   │                              │
   │                              │ 2. should_we_grant_deleg()?
   │                              │    - 检查 delegation 数量限制
   │                              │    - 检查客户端行为 (revoke count)
   │                              │    - 检查最近的 recall 历史
   │                              │
   │<── OK + READ_DELEGATION ─────│
   │                              │
```

### 4.2 回收 Delegation (CB_RECALL)

```
Client A                        Server                        Client B
   │                              │                              │
   │  (持有 Read Delegation)      │                              │
   │                              │<── OPEN(file, WRITE) ────────│
   │                              │                              │
   │                              │ 检测到冲突，需要回收
   │                              │
   │<── CB_RECALL(stateid) ───────│                              │
   │                              │                              │
   │ 1. 刷新本地缓存              │                              │
   │ 2. 发送 DELEGRETURN          │                              │
   │                              │                              │
   │── DELEGRETURN(stateid) ─────>│                              │
   │                              │                              │
   │                              │── OPEN OK ──────────────────>│
```

### 4.3 Delegation 超时回收

```
Client A                        Server
   │                              │
   │  (持有 Delegation)           │
   │                              │
   │<── CB_RECALL(stateid) ───────│
   │                              │
   │  (客户端无响应)              │
   │                              │
   │                              │ 等待 recall_timeout (30s)
   │                              │
   │                              │ 超时！强制回收 delegation
   │                              │ 标记客户端为 "misbehaving"
   │                              │ 增加 num_revokes 计数
```

## 5. NFS-Ganesha 对齐细节

### 5.1 should_we_grant_deleg() 启发式规则

参考 `state_deleg.c` 的 `should_we_grant_deleg()` 函数：

```rust
/// 决定是否应该授予 delegation
fn should_we_grant_deleg(
    file_stats: &FileStats,
    client: &ClientState,
    claim: OpenClaimType,
) -> (bool, WhyNoDeleg) {
    // 1. 检查全局开关
    if !config.allow_delegations {
        return (false, WND4_NOT_SUPP_FTYPE);
    }

    // 2. 检查 Backchannel 状态
    if client.backchannel_down() {
        match claim {
            CLAIM_PREVIOUS | CLAIM_DELEGATE_PREV => {
                // 恢复场景，允许但标记 pre-recall
                return (true, WND4_NONE);
            }
            _ => {
                return (false, WND4_RESOURCE);
            }
        }
    }

    // 3. 检查最近的 recall 历史 (RECALL2DELEG_TIME = 10s)
    if file_stats.last_recall != 0 
       && now() - file_stats.last_recall < 10 {
        return (false, WND4_CONTENTION);
    }

    // 4. 检查客户端行为
    if client.num_revokes > 2 {
        return (false, WND4_RESOURCE);
    }

    // 5. 检查写冲突
    if (access == READ && file_stats.num_write_opens > 0)
       || (access == WRITE && file_stats.num_write_opens > 1) {
        return (false, WND4_CONTENTION);
    }

    // 6. 检查全局 delegation 数量限制
    if file_stats.curr_delegations == 0 
       && g_total_num_files_delegated >= g_max_files_delegatable {
        return (false, WND4_RESOURCE);
    }

    (true, WND4_NONE)
}
```

### 5.2 Delegation 统计信息

参考 `file_deleg_stats` 结构：

```rust
/// 文件级别的 delegation 统计
pub struct FileStats {
    /// 当前活跃的 delegation 数量
    pub curr_delegations: u32,
    /// 当前 delegation 类型
    pub deleg_type: DelegationType,
    /// 历史授予次数
    pub delegation_count: u32,
    /// 历史回收次数
    pub recall_count: u32,
    /// 最后一次授予时间
    pub last_delegation: Instant,
    /// 最后一次回收时间
    pub last_recall: Instant,
    /// 平均持有时间
    pub avg_hold: Duration,
    /// 打开次数
    pub num_opens: u32,
    /// 写打开次数
    pub num_write_opens: u32,
}
```

## 6. Backchannel 实现

### 6.1 CB_COMPOUND 结构

```
CB_COMPOUND {
    tag: "recall",
    minorversion: 1,
    callback_ident: 0,
    operations: [
        CB_SEQUENCE {
            sessionid: client_session_id,
            sequenceid: next_cb_seq,
            slotid: 0,
            highest_slotid: 0,
            cachethis: false,
        },
        CB_RECALL {
            stateid: delegation_stateid,
            truncate: false,
            fh: file_handle,
        }
    ]
}
```

### 6.2 Backchannel 连接管理

```rust
pub struct BackchannelManager {
    /// Client ID -> Backchannel 连接
    channels: RwLock<HashMap<Clientid4, BackchannelConn>>,
}

pub struct BackchannelConn {
    /// 客户端的 callback 地址
    pub cb_addr: SocketAddr,
    /// Callback program number
    pub cb_program: u32,
    /// 当前 sequence ID
    pub sequence: AtomicU32,
    /// 连接状态
    pub state: AtomicU8,
}
```

## 7. 与 Curvine 的集成

### 7.1 当前状态

Curvine NFS Gateway 已有基础的 Delegation 框架：
- `delegation.rs`: DelegationManager 实现
- `backchannel.rs`: BackchannelManager 框架

### 7.2 需要增强的部分

1. **Backchannel 实际连接**：当前是空实现，需要实现真正的 RPC 回调
2. **文件统计信息**：需要跟踪 `FileStats` 用于启发式决策
3. **客户端行为跟踪**：需要跟踪 `num_revokes` 等指标
4. **Grace Period 集成**：服务器重启后的 delegation 恢复

### 7.3 配置选项

```rust
pub struct DelegationConfig {
    /// 是否启用 delegation (默认: false)
    pub enabled: bool,
    /// Recall 超时时间 (默认: 30s)
    pub recall_timeout_secs: u64,
    /// 最大 delegation 数量 (默认: 1000)
    pub max_delegations: usize,
    /// Reaper 检查间隔 (默认: 5000ms)
    pub reaper_check_interval_ms: u64,
}
```

## 8. 测试用例

| 用例 | 输入 | 预期结果 |
|------|------|----------|
| 授予 Read Delegation | OPEN(file, WANT_READ_DELEG) | 返回 READ_DELEGATION |
| 授予 Write Delegation | OPEN(file, WANT_WRITE_DELEG), 无其他打开 | 返回 WRITE_DELEGATION |
| 拒绝 (已有 delegation) | OPEN(file, WANT_DELEG), 文件已有 delegation | 返回 OPEN_DELEGATE_NONE |
| 回收 (写冲突) | Client A 持有 Read Deleg, Client B OPEN(WRITE) | CB_RECALL 发送给 A |
| 超时回收 | CB_RECALL 后 30s 无响应 | Delegation 被强制回收 |
| 客户端归还 | DELEGRETURN(stateid) | Delegation 被移除 |
| Lease 过期 | 客户端 lease 过期 | 所有 delegation 被回收 |

## 9. 性能考虑

### 9.1 何时启用 Delegation

**推荐启用的场景**：
- 读多写少的工作负载 (如编译、git status)
- 单客户端独占访问的文件
- 频繁 stat/getattr 的场景

**不推荐启用的场景**：
- 多客户端并发写入
- 高频率文件修改
- 对延迟敏感的写入操作

### 9.2 默认禁用的原因

1. **Backchannel 复杂性**：需要服务器主动连接客户端
2. **回收延迟**：CB_RECALL 需要等待客户端响应
3. **状态管理开销**：需要跟踪每个文件的 delegation 状态

## 10. 总结

Delegation 是一个强大的性能优化特性，但也增加了系统复杂性。当前 Curvine NFS Gateway 的实现已经有了基础框架，主要缺失的是：

1. **Backchannel 实际实现**：需要实现 RPC 回调机制
2. **启发式决策**：需要实现完整的 `should_we_grant_deleg()` 逻辑
3. **统计信息跟踪**：需要跟踪文件和客户端的统计信息

建议按以下优先级实现：
1. P0: 保持当前禁用状态，确保基本功能稳定
2. P1: 实现 Backchannel 连接管理
3. P2: 实现完整的启发式决策逻辑
4. P3: 添加 delegation 统计和监控

---

**文档版本**: 1.0  
**创建日期**: 2025-12-31  
**参考**: NFS-Ganesha `src/SAL/state_deleg.c`
