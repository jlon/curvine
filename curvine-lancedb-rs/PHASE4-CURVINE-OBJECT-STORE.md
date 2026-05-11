# Phase 4：Curvine Object Store 与 LanceDB 闭环

面向集成与环境核对：默认 CI 只跑不依赖真实集群的单元测试；需要 Curvine 集群与本机 `CURVINE_CONF_FILE` 时，请执行：

```text
CURVINE_CONF_FILE=/path/to/cluster.toml \
  cargo test -p curvine-lancedb-rs -- --ignored
```

## 已支持的语义

### `curvine://` URI：workspace_root / extract_path / object_path（同一套规则）

实现以 `curvine_absolute_path_str_from_uri` 为唯一合并入口：`authority` 非空时作为绝对路径的首段，`path` 接在后面；`authority` 为空时只用 `path`。

| URI | 合并后的 workspace 绝对路径 |
|-----|------------------------------|
| `curvine:///data/db` | `/data/db` |
| `curvine://tenant/data/db` | `/tenant/data/db` |
| `curvine://tenant` | `/tenant` |

- **workspace_root**：上表的绝对路径，即数据集根。
- **extract_path**：用同一套合并规则校验 URI，通过后返回**空**对象存储路径。Lance `from_uri_and_params` 把整条数据集 URI 交给 `new_store`，后续键均为相对该 workspace 的片段；**不得**把 `Url::path()` 单独当成 object key，否则会把 `tenant` 留在 workspace 里却从 key 里丢掉。
- **object_path**：`workspace_root` + 相对键；若上游 `location` 误带与 workspace 尾部重复的前缀，会 `strip_prefix` 纠偏。

### `copy`（阶段性语义）

- 实现为**非原子** read（按块）+ write，不是服务端单次字节级 copy。
- 目标路径使用 Curvine `create(..., overwrite = true)`：**已存在则覆盖**；**源对象保留**（copy，非 move）。
- 与「完整 ObjectStore copy 语义（例如跨存储原子、copy_if_not_exists）」未对齐的部分，以本段与 `object_store.rs` 上 `copy` 的文档注释为准。

### LanceDB（`curvine://`）

- `connect`（通过 `ConnectBuilder` + `storage_option(curvine.conf.path, …)`）。
- `create_table` / `open_table`、写入初始 `RecordBatch`、`query().execute()`、`count_rows`。
- 底层使用 Curvine 提供的 [`object_store`](https://docs.rs/object_store/) 接口：`head`、`get`（含 `head` 只取元数据、`range`）、`put`（覆盖写）、`delete`、`list`（递归枚举）、`list_with_delimiter`、`copy`。

### ObjectMeta（Curvine → Lance）

| 字段 | 来源 | 说明 |
|------|------|------|
| `location` | 对象键（相对 workspace） | 与 object-store 路径一致 |
| `size` | `FileStatus.len` | 字节数 |
| `last_modified` | `FileStatus.mtime`（毫秒） | 转成 UTC |
| `e_tag` | **合成弱标签** `W/"cv:{inode}:{mtime_ms}"` | **不是**内容哈希；可用于 inode+mtime 级别的稳定性，不适合字节级 If-Match |
| `version` | 固定 `None` | 未暴露 Curvine 版本号 |

### FsError → object_store::Error

稳定映射（按枚举分支，**不**依赖错误文案 `contains`）：

| FsError 变体（节选） | object_store::Error |
|---------------------|---------------------|
| `FileNotFound`, `Expired`, `JobNotFound` | `NotFound` |
| `FileAlreadyExists` | `AlreadyExists` |
| `Unsupported`, `UnsupportedUfsRead` | `NotSupported` |
| 其余 | `Generic`（`store = "curvine"`） |

## 明确未支持 / 阶段性边界

- **`put_multipart` / 分片上传**：返回 `NotImplemented`。单次 `put` 走 Curvine 流式写入；**大对象**是否拆分 multipart 属于后续阶段，需对齐 `curvine-s3-gateway` 临时写、`s3s-fs` 落盘等策略后再实现。
- **对象版本号**：`ObjectMeta.version` 恒为 `None`；若将来支持条件请求，需在服务端暴露版本或内容哈希后再接 `If-Match` / S3 风格版本。
- **内容级 e_tag**：当前仅为弱合成 etag；若要做严格缓存校验，需要 FS 或网关提供 MD5/SHA 或等价摘要。
- **单次写入上限**：受 Curvine 客户端单次写入与块大小约束；未在本 crate 内单独限制。超大对象建议后续通过 multipart + staging 路径接入。

## 下一阶段（候选）

1. Multipart / 大对象：复用团队现有网关与 FS staging 模式，接入 `put_multipart_opts`。
2. 元数据加固：可选真实 `e_tag`（内容哈希）、可选 `version`（快照 id）。
3. 条件 GET：`get_opts` 已与 `check_preconditions` 对齐；待 etag/version 语义就绪后补集成测试。
