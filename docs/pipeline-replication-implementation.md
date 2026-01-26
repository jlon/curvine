# Curvine Pipeline 复制实现文档（小白版）

## 1. 这篇文档讲什么

用最简单的话解释 Curvine 的 Pipeline 副本写入是怎么实现的，以及为什么这样做。

如果你只需要记住一句话：
**客户端只发一份数据，Worker 们像接力一样往下传，最后所有副本都写成。**

---

## 2. Pipeline 是什么

传统多副本写入：客户端要把同一份数据发给多个 Worker（比如 3 次）。

Pipeline 写入：客户端只发一次给“头节点”，然后由头节点转发给下游 Worker。  
好处是 **节省客户端带宽**，缺点是 **延迟会累积**。

```
Client → Worker1(Head) → Worker2 → Worker3(Tail)
```

---

## 3. Pipeline 的核心流程

为了小白更容易理解，这里加入一个简化流程图。

```
建立阶段：
Client -> Head(Open, downstream) -> Next -> ... -> Tail
Tail 成功返回 -> 逐层返回 -> Client 得到建立状态

写入阶段：
Client -> Head(写本地 + 转发) -> ... -> Tail(写本地)
Tail ACK -> 逐层 ACK -> Client

完成阶段：
Client -> Head(Complete) -> ... -> Tail(Complete)
Client -> Master(CommitBlock)
```

### 3.1 建立 Pipeline（Open 阶段）

1. Client 向 Head 发送 Open 请求，带上 downstream 列表（后面的 Worker）。
2. Head 依次向下游建立连接。
3. Tail 成功后，成功信息逐层返回。
4. Client 得到一个 “建立状态”，知道哪些节点成功了。

如果成功节点数 < min_replicas，就直接失败。  
如果成功节点数 >= min_replicas 但 < 期望副本数，就进入“降级继续”。

---

### 3.2 写数据（Running 阶段）

每次写入一个 chunk：

1. Head 先写本地（或 short_circuit 本地写）。
2. 同时把数据转发给下游。
3. Tail 写完后，ACK 从后往前返回。
4. Client 收到 ACK 才认为这一块数据成功。

---

### 3.3 完成写入（Complete 阶段）

Client 调用 Complete：
1. Head 通知下游完成。
2. 生成 CommitBlock（包含成功的副本位置）。
3. Master 记录这些副本位置。

如果有副本在完成阶段失败，系统接受最终一致性，
后续由修复性复制补齐。

---

## 4. 关键实现组件（通俗解释）

### 4.1 Client 端

- `BlockWriter`：写入总管，负责：
  - 发数据
  - 处理失败
  - 降级或重建
  - 记录哪些副本成功

- `BlockWriterRemote`：负责和单个远程 Worker 通信。

- `BlockWriterLocal`：本地 short_circuit 写入。

---

### 4.2 Worker 端

- `WriteHandler`：接收写请求，写本地，同时转发下游。

---

## 5. 失败时怎么办（很重要）

### 5.1 建立阶段失败

如果建立 Pipeline 时有节点连不上：
1. 看成功副本数是否满足 `min_replicas`
2. 满足 → 继续写（降级）
3. 不满足 → 直接失败

### 5.2 写入阶段失败

写入过程中某个节点挂了：
1. 剔除失败节点
2. 尝试请求替换节点
3. 必要时重放已写数据
4. 如果剩余副本数 >= min_replicas，则允许降级继续写

---

## 6. 为什么还需要“修复性复制”

如果写入过程中降级了（比如目标 3 副本，实际只写成 2 副本），  
系统允许这次写入先成功返回，但后台必须把副本补齐。

这就是“修复性复制”：

1. Client 提交 CommitBlock（只包含成功的副本位置）
2. Master 发现副本数不足
3. Master 调度复制任务
4. 把已有副本的数据复制到新节点

这样可以把“写入可用性”和“最终副本数”分离，提升稳定性。

---

## 7. 重放数据（Replay）流程

当写入过程中发生故障并触发替换节点时，新的节点没有之前的数据，需要重放：

1. 写入时会把已发送的 chunk 暂存在 replay 缓冲区  
2. 故障发生，剔除失败节点，尝试替换  
3. 替换成功后，把 replay 缓冲区里的数据重新写给新节点  
4. 如果 replay 缓冲区超过限制，会被关闭（避免内存过大），此时不再重放

这是“少量重放 + 内存可控”的实现折中。

---

## 8. 最重要的配置参数（简化版）

以下是当前实现中最常见的参数，只保留理解所需的部分：

- `client.pipeline_timeout_ms`  
  Pipeline 写入阶段超时时间。为 0 时回落到 `client.data_timeout_ms`。

- `client.data_timeout_ms`  
  普通写入超时时间。

- `master.min_replication`  
  最小副本数。低于该值直接失败。

- `client.write_chunk_size` + `client.write_chunk_num`  
  影响 replay 缓冲区大小（限制内存占用）。

---

## 9. 读写成功的判断（很关键）

Client 认为某个 chunk 写成功的条件：

1. Head 本地写成功
2. 所有下游都返回成功 ACK
3. 整条链路都成功，才算这一块成功

如果只是部分成功，不会算成功（除非进入降级模式后继续）。

---

## 10. 常见误解

- Pipeline 不是为了降低延迟，而是为了节省客户端带宽。
- ACK 不是“本地写成功”，而是“整条链路成功”。
- 降级不是最终状态，必须靠后台修复性复制补齐副本。

---

## 11. FAQ（根据你的提问）

**Q1：RecoveryHandler 的作用是啥？为啥需要 Recovery？**

A：它负责“写入失败后的统一恢复流程”。  
当某个节点失败时，必须做很多事：剔除失败节点、尝试替换、检查 min_replicas、必要时重放数据。  
如果这些逻辑散落在 `BlockWriter` 里，会变得非常复杂，所以把恢复逻辑封装成 `RecoveryHandler`，让 `BlockWriter` 只负责流程调度，逻辑更清晰、风险更小。

**Q2：UnderReplicatedReporter 的作用以及后续流程是啥？**

A：它负责“上报副本不足”。  
当副本数低于目标值（或低于 min_replicas）时，它会向 Master 报告。  
Master 收到后，会把该 Block 放进修复队列，调度复制任务补齐副本。  
这就是“事件驱动的修复性复制”。

**Q3：现在客户端写入成功，是三个副本都成功才表示成功吗？**

A：是。  
在正常模式下，只有三个副本都成功 ACK，Client 才认为写成功。  
如果进入“降级模式”，当副本数 >= min_replicas 时可以继续写，但这属于“降级成功”，不是满副本成功。  
最终仍会由修复性复制补齐到目标副本数。

**Q4：如果设置 3 副本但只启动 1 个 Worker，能成功吗？**

A：取决于 `min_replication`。  
如果 `min_replication > 1`，建立阶段就会失败。  
如果 `min_replication == 1`，可以进入“降级模式”，写入允许成功，但只会有 1 个副本，后续由修复性复制补齐。

**Q5：写数据流程需要加流程图和核心代码吗？**

A：需要。  
已经加入简化流程图，核心代码建议只保留“关键路径”的伪代码或简化片段，避免把实现细节塞满文档。

**Q6：重放数据流程再描述一下**

A：已在“重放数据（Replay）流程”章节说明。  
核心逻辑是“写入时缓存 → 替换成功后重放 → 缓冲区超限则关闭重放”。

