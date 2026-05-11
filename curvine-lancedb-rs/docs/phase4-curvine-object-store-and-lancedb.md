# Phase 4：Curvine Object Store 与 LanceDB

面向 reviewer：先读本文再对照 `tests/` 下 `#[ignore]` 集成用例；默认 CI 只跑**不依赖集群**的单测与 facade 测试。

---

## Phase 4A（object-store MVP，已实现）

### `curvine://` URI：workspace_root / extract_path / object_path

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
- `copy_if_not_exists`：**未实现**（`NotImplemented`）。

### LanceDB facade（`curvine://`）

- `connect` / `ConnectBuilder` + `storage_option(curvine.conf.path, …)`（见 `CURVINE_CONF_FILE_KEY`）。
- 底层 Curvine [`object_store`](https://docs.rs/object_store/)：`head`、`get`（含 `head=true` 元数据、`range`）、`put`（覆盖写）、`delete`、递归 `list`、`list_with_delimiter`、`copy`。

### ObjectMeta（Curvine → Lance）

| 字段 | 来源 | 说明 |
|------|------|------|
| `location` | 对象键（相对 workspace） | 与 object-store 路径一致 |
| `size` | `FileStatus.len` | 字节数 |
| `last_modified` | `FileStatus.mtime`（毫秒） | 转成 UTC |
| `e_tag` | **合成弱标签** `W/"cv:{inode}:{mtime_ms}"` | **不是**内容哈希；不适合字节级 If-Match |
| `version` | 固定 `None` | 未暴露 Curvine 版本号 |

### FsError → object_store::Error

按枚举分支映射（**不**依赖错误文案 `contains`）：`FileNotFound` / `Expired` / `JobNotFound` → `NotFound`；`FileAlreadyExists` → `AlreadyExists`；`Unsupported` / `UnsupportedUfsRead` → `NotSupported`；其余 → `Generic`（`store = "curvine"`）。

---

## Phase 4B（真实语义验证，本阶段正式测试）

### 运行方式（live cluster）

需可达 Curvine 集群，并设置 `CURVINE_CONF_FILE` 指向客户端集群配置（与 `ClusterConf::ENV_CONF_FILE` 一致）。

```text
export CURVINE_CONF_FILE=/path/to/cluster.toml

cargo test -p curvine-lancedb-rs --test object_store_semantics -- --ignored
cargo test -p curvine-lancedb-rs --test lancedb_smoke -- --ignored
```

### 测试清单（与代码一一对应）

| 文件 | 作用 |
|------|------|
| `tests/object_store_semantics.rs` | 单测 `CurvineObjectStore`：`put`、`head`、`get_opts(head=true)`、range `get_opts`、错误 range、overwrite `put`、`copy`（源保留 + 目标已存在时覆盖）、`delete`、递归 `list`、`list_with_delimiter`（目录前缀不得作为 file object）；`delete` 后 `list` 不再包含被删 key。 |
| `tests/lancedb_smoke.rs` | LanceDB 路径：`connect(curvine://…)` → `create_table`（带 `storage_option`）→ `table_names` → `open_table` → `count_rows` → `query().execute()` 拉 batch。 |
| `tests/facade_compat.rs` | 无集群：registry、本地 connect、无配置时 connect 失败文案等。 |

### 明确未纳入本阶段

- **`curvine-tests` MiniCluster e2e**（`lancedb_minicluster_e2e` 等）：未在本仓库分支作为 Phase 4B 交付物；若纳入需单独 PR、跑通并维护 CI 依赖与 `Cargo.lock` 边界。

---

## 明确未支持 / 阶段性边界

- **`put_multipart` / 分片上传**：返回 `NotImplemented`。单次 `put` 走 Curvine 流式写入。
- **条件写 / 对象版本**：未暴露 `ObjectMeta.version`；无完整条件 PUT/POST 语义。
- **内容级 e_tag / 内容哈希**：当前仅为弱合成 etag；无 MD5/SHA 级强校验。
- **单次写入上限**：受 Curvine 客户端与块大小约束；本 crate 未单独限制。

---

## 下一阶段（候选）

1. Multipart / 大对象：对齐网关与 FS staging 后实现 `put_multipart_opts`。
2. 元数据：可选真实 `e_tag`、`version`。
3. MiniCluster 或 CI 集成：在可控环境内自动化跑 `#[ignore]` 用例（或拆出 `slow` feature）。
