# NFSv4 Gateway 日志优化方案

## 问题分析

当前日志存在以下问题：
1. **高频操作使用 INFO 级别**：COMPOUND、READ、WRITE、GETATTR 等每次都打印
2. **重复日志**：每个操作的请求和响应都打印详细信息
3. **状态持久化日志过多**：每 30 秒打印一次
4. **错误日志重复**：同一个错误多次打印

## 优化原则

### 日志级别定义
- **ERROR**: 错误情况，需要人工介入
- **WARN**: 警告情况，可能影响功能但不致命
- **INFO**: 重要状态变更（OPEN/CLOSE、CREATE/REMOVE、客户端注册）
- **DEBUG**: 高频操作（COMPOUND、READ/WRITE、GETATTR、RENEW）
- **TRACE**: 详细调试信息（参数、返回值）

### 优化策略

#### 1. 高频操作降级为 DEBUG
- COMPOUND 请求/响应
- READ/WRITE 操作
- GETATTR/SETATTR 操作
- RENEW 操作（心跳）
- LOOKUP 操作
- ACCESS 操作

#### 2. 状态变更保持 INFO
- OPEN/CLOSE（文件打开/关闭）
- CREATE/REMOVE（文件创建/删除）
- MKDIR/RMDIR（目录创建/删除）
- RENAME（文件重命名）
- LINK/SYMLINK（链接创建）
- 客户端注册/确认
- Session 创建/销毁

#### 3. 状态持久化优化
- 降级为 DEBUG
- 只在失败时打印 ERROR

#### 4. 错误日志去重
- 使用 rate limiting
- 合并重复错误

## 需要修改的文件

### 核心文件
1. `curvine-nfs/src/nfs4/handlers.rs` - COMPOUND 处理
2. `curvine-nfs/src/nfs4/ops/read.rs` - READ 操作
3. `curvine-nfs/src/nfs4/ops/write.rs` - WRITE 操作
4. `curvine-nfs/src/nfs4/ops/getattr.rs` - GETATTR 操作
5. `curvine-nfs/src/nfs4/ops/setattr.rs` - SETATTR 操作
6. `curvine-nfs/src/nfs4/ops/lookup.rs` - LOOKUP 操作
7. `curvine-nfs/src/nfs4/ops/access.rs` - ACCESS 操作
8. `curvine-nfs/src/nfs4/state/persistence.rs` - 状态持久化

### 状态管理文件
9. `curvine-nfs/src/nfs4/state/open.rs` - OPEN/CLOSE（保持 INFO）
10. `curvine-nfs/src/nfs4/state/client.rs` - 客户端管理（保持 INFO）

## 预期效果

优化前（INFO 级别）：
- 每秒约 1000+ 条日志
- 日志文件增长速度：~10 MB/分钟

优化后（INFO 级别）：
- 每秒约 10-50 条日志
- 日志文件增长速度：~1 MB/分钟
- **减少 90% 的日志输出**

## 实施步骤

1. ✅ 优化 handlers.rs - COMPOUND 请求/响应
2. 优化 ops/*.rs - 高频操作
3. 优化 state/persistence.rs - 状态持久化
4. 测试验证
5. 更新文档

---
*创建时间: 2025-12-30 14:20*
