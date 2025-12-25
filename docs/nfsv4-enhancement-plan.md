# NFSv4 功能增强实施计划

## 总体目标

参考 NFS-Ganesha 实现，完善 Curvine NFSv4 的生产级功能：
1. 完善锁管理（生产环境必需）
2. 实现委托机制（显著提升性能）
3. Grace Period 和状态恢复（容错能力）
4. 完善回调通道（支持高级特性）

## 第一阶段：完善锁管理（最高优先级）

### 当前问题
- ✅ 基础锁状态跟踪
- ❌ 无真正的锁冲突检测
- ❌ 无锁队列管理
- ❌ 无锁升级/降级
- ❌ 无死锁检测

### NFS-Ganesha 锁管理架构（参考）
```c
// 核心数据结构
state_lock_entry_t {
    - lock_type: READ/WRITE
    - offset, length: 锁范围
    - owner: 锁所有者
    - state: GRANTED/BLOCKED
    - blocking_list: 被阻塞的锁队列
}

// 核心功能
1. 锁冲突检测: check_lock_conflict()
2. 锁授予: grant_lock()
3. 锁阻塞: block_lock()
4. 锁升级: upgrade_lock()
5. 锁降级: downgrade_lock()
6. 死锁检测: detect_deadlock()
```

### 实施步骤

#### 1.1 重构 LockManager 数据结构
```rust
// 当前简化版本
pub struct LockManager {
    locks: RwLock<HashMap<[u8; 12], LockState>>,
}

// 目标完整版本
pub struct LockManager {
    // File -> List of locks (按 offset 排序)
    file_locks: RwLock<HashMap<Fileid4, Vec<Arc<LockEntry>>>>,
    // Stateid -> Lock entry
    lock_states: RwLock<HashMap<[u8; 12], Arc<LockEntry>>>,
    // Blocked locks queue
    blocked_locks: RwLock<Vec<Arc<BlockedLock>>>,
    // Lock owner -> locks
    owner_locks: RwLock<HashMap<LockOwnerId, Vec<[u8; 12]>>>,
}

pub struct LockEntry {
    stateid: Stateid4,
    fileid: Fileid4,
    lock_type: LockType,  // READ/WRITE
    offset: u64,
    length: u64,  // 0 = EOF
    owner: LockOwnerId,
    state: LockState,  // GRANTED/BLOCKED
    granted_time: Option<SystemTime>,
}

pub struct BlockedLock {
    lock_entry: Arc<LockEntry>,
    blocking_locks: Vec<[u8; 12]>,  // 阻塞它的锁
    block_time: SystemTime,
    callback_sent: bool,
}
```

#### 1.2 实现锁冲突检测
```rust
impl LockManager {
    /// Check if a lock conflicts with existing locks
    fn check_conflict(
        &self,
        fileid: Fileid4,
        lock_type: LockType,
        offset: u64,
        length: u64,
        exclude_owner: Option<&LockOwnerId>,
    ) -> Option<Vec<Arc<LockEntry>>> {
        // 1. 获取文件的所有锁
        // 2. 检查范围重叠
        // 3. 检查类型冲突（WRITE vs READ/WRITE）
        // 4. 排除同一所有者的锁
        // 5. 返回冲突的锁列表
    }
    
    /// Check if two lock ranges overlap
    fn ranges_overlap(
        offset1: u64, length1: u64,
        offset2: u64, length2: u64,
    ) -> bool {
        // Handle EOF (length = 0)
        // Check overlap
    }
}
```

#### 1.3 实现锁队列管理
```rust
impl LockManager {
    /// Try to grant a lock, or block it if conflicts exist
    pub async fn acquire_lock(
        &self,
        fileid: Fileid4,
        lock_type: LockType,
        offset: u64,
        length: u64,
        owner: LockOwnerId,
    ) -> Result<Arc<LockEntry>, BlockedLockInfo> {
        // 1. 检查冲突
        if let Some(conflicts) = self.check_conflict(...) {
            // 2. 创建阻塞锁
            let blocked = self.create_blocked_lock(...);
            return Err(BlockedLockInfo { conflicts, blocked });
        }
        
        // 3. 授予锁
        let lock = self.grant_lock(...);
        
        // 4. 检查是否可以唤醒被阻塞的锁
        self.try_grant_blocked_locks(fileid).await;
        
        Ok(lock)
    }
    
    /// Try to grant blocked locks after a lock is released
    async fn try_grant_blocked_locks(&self, fileid: Fileid4) {
        // 1. 获取该文件的所有阻塞锁
        // 2. 按 FIFO 顺序尝试授予
        // 3. 如果授予成功，发送 CB_NOTIFY_LOCK 回调
    }
}
```

#### 1.4 实现锁升级/降级
```rust
impl LockManager {
    /// Upgrade a READ lock to WRITE lock
    pub fn upgrade_lock(
        &self,
        stateid: &Stateid4,
    ) -> Nfs4Result<()> {
        // 1. 验证当前是 READ 锁
        // 2. 检查是否有其他 READ 锁（冲突）
        // 3. 如果有冲突，阻塞
        // 4. 否则升级为 WRITE 锁
    }
    
    /// Downgrade a WRITE lock to READ lock
    pub fn downgrade_lock(
        &self,
        stateid: &Stateid4,
    ) -> Nfs4Result<()> {
        // 1. 验证当前是 WRITE 锁
        // 2. 降级为 READ 锁
        // 3. 尝试授予被阻塞的 READ 锁
    }
}
```

### 测试用例
1. 基础锁冲突：WRITE vs WRITE
2. 读写冲突：WRITE vs READ
3. 多个 READ 锁共存
4. 锁队列：先到先得
5. 锁升级：READ -> WRITE
6. 锁降级：WRITE -> READ
7. 范围锁：部分重叠
8. EOF 锁：length = 0

---

## 第二阶段：实现委托机制

### 当前问题
- ⚠️ 有 DelegationManager 框架但功能不完整
- ❌ 无读委托实现
- ❌ 无写委托实现
- ❌ 无委托召回（CB_RECALL）
- ❌ 无委托冲突检测

### NFS-Ganesha 委托架构（参考）
```c
// 核心数据结构
state_deleg_t {
    - type: READ/WRITE
    - fileid: 文件ID
    - clientid: 客户端ID
    - stateid: 委托状态ID
    - recall_in_progress: 是否正在召回
}

// 核心功能
1. 授予委托: grant_delegation()
2. 召回委托: recall_delegation() -> CB_RECALL
3. 返回委托: return_delegation()
4. 冲突检测: check_delegation_conflict()
```

### 实施步骤

#### 2.1 完善 DelegationManager
```rust
pub struct DelegationManager {
    // File -> Delegations
    file_delegations: RwLock<HashMap<Fileid4, Vec<Arc<Delegation>>>>,
    // Stateid -> Delegation
    delegations: RwLock<HashMap<[u8; 12], Arc<Delegation>>>,
    // Client -> Delegations
    client_delegations: RwLock<HashMap<Clientid4, Vec<[u8; 12]>>>,
    // Backchannel for CB_RECALL
    backchannel: Arc<BackchannelManager>,
}

pub struct Delegation {
    stateid: Stateid4,
    fileid: Fileid4,
    clientid: Clientid4,
    deleg_type: DelegationType,  // READ/WRITE
    granted_time: SystemTime,
    recall_in_progress: AtomicBool,
    recall_time: RwLock<Option<SystemTime>>,
}

pub enum DelegationType {
    Read,
    Write,
}
```

#### 2.2 实现读委托
```rust
impl DelegationManager {
    /// Try to grant a READ delegation
    pub fn try_grant_read_delegation(
        &self,
        fileid: Fileid4,
        clientid: Clientid4,
    ) -> Option<Arc<Delegation>> {
        // 1. 检查是否已有 WRITE 委托（冲突）
        // 2. 检查是否有其他客户端的 WRITE 打开（冲突）
        // 3. 如果无冲突，授予 READ 委托
        // 4. 客户端可以缓存读取数据
    }
}
```

#### 2.3 实现写委托
```rust
impl DelegationManager {
    /// Try to grant a WRITE delegation
    pub fn try_grant_write_delegation(
        &self,
        fileid: Fileid4,
        clientid: Clientid4,
    ) -> Option<Arc<Delegation>> {
        // 1. 检查是否已有任何委托（WRITE 委托是独占的）
        // 2. 检查是否有其他客户端的打开（冲突）
        // 3. 如果无冲突，授予 WRITE 委托
        // 4. 客户端可以缓存写入，延迟提交
    }
}
```

#### 2.4 实现委托召回
```rust
impl DelegationManager {
    /// Recall a delegation (send CB_RECALL)
    pub async fn recall_delegation(
        &self,
        delegation: &Arc<Delegation>,
        reason: RecallReason,
    ) -> Nfs4Result<()> {
        // 1. 标记为正在召回
        delegation.recall_in_progress.store(true, Ordering::Release);
        
        // 2. 发送 CB_RECALL 回调
        self.backchannel.send_cb_recall(
            delegation.clientid,
            delegation.stateid,
            delegation.fileid,
        ).await?;
        
        // 3. 等待客户端返回委托（DELEGRETURN）
        // 4. 如果超时，强制撤销
    }
    
    /// Check if need to recall delegations before an operation
    pub async fn check_and_recall_conflicts(
        &self,
        fileid: Fileid4,
        operation: Operation,
        clientid: Clientid4,
    ) -> Nfs4Result<()> {
        // 例如：其他客户端要 WRITE，需要召回所有 READ 委托
    }
}
```

### 性能提升预期
- 读密集场景：减少 50-80% 的 READ RPC
- 写密集场景：减少 30-50% 的 WRITE RPC
- 混合场景：减少 40-60% 的总 RPC

---

## 第三阶段：Grace Period 和状态恢复

### 当前问题
- ❌ 无 Grace Period 管理
- ❌ 无状态回收机制
- ❌ 无网络分区恢复

### NFS-Ganesha Grace Period 架构
```c
// Grace Period 状态机
NORMAL -> GRACE -> NORMAL

// 触发条件
1. 服务器重启
2. 网络分区恢复
3. 手动触发

// 期间行为
1. 只允许 RECLAIM 操作
2. 拒绝新的 LOCK/OPEN
3. 超时后进入 NORMAL
```

### 实施步骤

#### 3.1 实现 GracePeriodManager
```rust
pub struct GracePeriodManager {
    state: RwLock<GraceState>,
    start_time: RwLock<Option<SystemTime>>,
    duration: Duration,  // 默认 90 秒
}

pub enum GraceState {
    Normal,
    Grace,
}

impl GracePeriodManager {
    /// Enter grace period (on server restart)
    pub fn enter_grace_period(&self) {
        // 1. 设置状态为 GRACE
        // 2. 记录开始时间
        // 3. 启动定时器
    }
    
    /// Check if in grace period
    pub fn in_grace_period(&self) -> bool {
        // 检查状态和超时
    }
    
    /// Allow operation during grace period
    pub fn allow_operation(&self, op: Operation) -> bool {
        match self.state.read().unwrap() {
            GraceState::Normal => true,
            GraceState::Grace => {
                // 只允许 RECLAIM 操作
                matches!(op, Operation::Reclaim)
            }
        }
    }
}
```

#### 3.2 实现状态回收
```rust
impl ClientManager {
    /// Mark client for reclaim
    pub fn mark_for_reclaim(&self, clientid: Clientid4) {
        // 客户端重新连接时，标记需要回收状态
    }
    
    /// Reclaim state (LOCK/OPEN)
    pub fn reclaim_state(
        &self,
        clientid: Clientid4,
        state_type: StateType,
        state_data: StateData,
    ) -> Nfs4Result<()> {
        // 1. 验证在 Grace Period 内
        // 2. 验证客户端有权回收
        // 3. 恢复状态
    }
}
```

---

## 第四阶段：完善回调通道

### 当前问题
- ⚠️ 有 BackchannelManager 框架
- ❌ 无 CB_RECALL 实现
- ❌ 无 CB_GETATTR 实现
- ❌ 无 CB_NOTIFY_LOCK 实现

### 实施步骤

#### 4.1 实现 CB_RECALL
```rust
impl BackchannelManager {
    pub async fn send_cb_recall(
        &self,
        clientid: Clientid4,
        stateid: Stateid4,
        fileid: Fileid4,
    ) -> Nfs4Result<()> {
        // 1. 构造 CB_RECALL 请求
        // 2. 通过回调通道发送
        // 3. 等待响应或超时
    }
}
```

#### 4.2 实现 CB_NOTIFY_LOCK
```rust
impl BackchannelManager {
    pub async fn send_cb_notify_lock(
        &self,
        clientid: Clientid4,
        lock_owner: LockOwnerId,
        fileid: Fileid4,
    ) -> Nfs4Result<()> {
        // 通知客户端锁已授予
    }
}
```

---

## 实施优先级和时间估算

### 第一阶段：锁管理（3-5天）
- Day 1: 重构数据结构
- Day 2: 实现冲突检测
- Day 3: 实现锁队列
- Day 4: 实现升级/降级
- Day 5: 测试验证

### 第二阶段：委托机制（5-7天）
- Day 1-2: 完善 DelegationManager
- Day 3: 实现读委托
- Day 4: 实现写委托
- Day 5: 实现召回机制
- Day 6-7: 测试验证

### 第三阶段：Grace Period（2-3天）
- Day 1: 实现 GracePeriodManager
- Day 2: 实现状态回收
- Day 3: 测试验证

### 第四阶段：回调通道（2-3天）
- Day 1: 实现 CB_RECALL
- Day 2: 实现 CB_NOTIFY_LOCK
- Day 3: 测试验证

**总计：12-18 天**

---

## 成功标准

### 锁管理
- ✅ 通过所有锁冲突测试
- ✅ 支持锁队列和阻塞
- ✅ 支持锁升级/降级
- ✅ 无死锁

### 委托机制
- ✅ 读委托正常工作
- ✅ 写委托正常工作
- ✅ 召回机制正常
- ✅ 性能提升 40%+

### Grace Period
- ✅ 服务器重启后正常恢复
- ✅ 状态回收正常
- ✅ 无状态丢失

### 回调通道
- ✅ CB_RECALL 正常工作
- ✅ CB_NOTIFY_LOCK 正常工作
- ✅ 超时处理正常
