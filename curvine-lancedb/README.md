# LanceDB on Curvine

## 中文说明

`curvine-lancedb` 是 LanceDB on Curvine 的 Rust facade crate。Cargo 包名是
`curvine-lancedb`，但 Rust crate 名仍然是 `lancedb`，所以业务代码可以继续使用
`use lancedb::connect` 这类上游 LanceDB 风格的导入。

### 依赖引入

当前不发布 crates.io，业务侧使用 Git 方式引入：

```toml
[dependencies]
lancedb = { package = "curvine-lancedb", git = "https://github.com/CurvineIO/curvine", branch = "feat/lancedb-on-curvine" }
```

生产业务建议固定 commit，避免分支继续变化影响构建：

```toml
[dependencies]
lancedb = { package = "curvine-lancedb", git = "https://github.com/CurvineIO/curvine", rev = "<commit-sha>" }
```

### Curvine 配置

业务需要提供 Curvine client 配置文件。三 master 示例：

```toml
[client]
master_addrs = [
    { hostname = "10.209.148.124", port = 8995 },
    { hostname = "10.209.148.125", port = 8995 },
    { hostname = "10.209.148.127", port = 8995 },
]
```

推荐在连接时显式传入配置路径：

```rust,no_run
use lancedb::connect;
use lancedb::object_store::CURVINE_CONF_FILE_KEY;

# async fn example() -> lancedb::Result<()> {
let db = connect("curvine:///data/lancedb/demo")
    .storage_option(CURVINE_CONF_FILE_KEY, "/path/to/curvine-cluster.toml")
    .execute()
    .await?;
# Ok(())
# }
```

也可以设置环境变量 `CURVINE_CONF_FILE=/path/to/curvine-cluster.toml`。如果两者同时存在，
`storage_option(CURVINE_CONF_FILE_KEY, ...)` 优先级更高。

### 最小示例

```rust,no_run
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::connect;
use lancedb::object_store::CURVINE_CONF_FILE_KEY;
use lancedb::query::ExecutableQuery;

# async fn example() -> lancedb::Result<()> {
let conf = "/path/to/curvine-cluster.toml";
let db = connect("curvine:///data/lancedb/demo")
    .storage_option(CURVINE_CONF_FILE_KEY, conf)
    .execute()
    .await?;

let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
let batch = RecordBatch::try_new(
    schema,
    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
)?;

let table = db
    .create_table("items", batch)
    .storage_option(CURVINE_CONF_FILE_KEY, conf)
    .execute()
    .await?;

let rows = table.query().execute().await?.try_collect::<Vec<_>>().await?;
assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
# Ok(())
# }
```

### 使用约束

- 使用 `curvine://` URI，不需要 FUSE。
- workspace root 是 Curvine 上的目录，例如 `curvine:///data/lancedb/demo`。
- 非 Curvine 的 LanceDB API 继续委托给上游 LanceDB。
- 当前已使用 `10.209.148.124:8995,10.209.148.125:8995,10.209.148.127:8995`
  跑通 `curvine-tests` LanceDB object-store e2e。

## English

`curvine-lancedb` is a facade crate that lets Rust applications use LanceDB on
Curvine through Lance's object store interface. The Cargo package is
`curvine-lancedb`, but the Rust library name is still `lancedb`, so application
code can keep upstream-style imports.

## Add The Dependency

Use Git while this crate is not published to crates.io:

```toml
[dependencies]
lancedb = { package = "curvine-lancedb", git = "https://github.com/CurvineIO/curvine", branch = "feat/lancedb-on-curvine" }
```

If the business project needs a fixed revision, pin the commit instead of the
branch:

```toml
[dependencies]
lancedb = { package = "curvine-lancedb", git = "https://github.com/CurvineIO/curvine", rev = "<commit-sha>" }
```

## Provide Curvine Configuration

Create a Curvine client config file and point LanceDB at it. For a three-master
cluster:

```toml
[client]
master_addrs = [
    { hostname = "10.209.148.124", port = 8995 },
    { hostname = "10.209.148.125", port = 8995 },
    { hostname = "10.209.148.127", port = 8995 },
]
```

The application can pass the config path per connection:

```rust,no_run
use lancedb::connect;
use lancedb::object_store::CURVINE_CONF_FILE_KEY;

# async fn example() -> lancedb::Result<()> {
let db = connect("curvine:///data/lancedb/demo")
    .storage_option(CURVINE_CONF_FILE_KEY, "/path/to/curvine-cluster.toml")
    .execute()
    .await?;
# Ok(())
# }
```

It can also set `CURVINE_CONF_FILE=/path/to/curvine-cluster.toml`. The explicit
storage option has higher priority.

## Minimal Usage

```rust,no_run
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::connect;
use lancedb::object_store::CURVINE_CONF_FILE_KEY;
use lancedb::query::ExecutableQuery;

# async fn example() -> lancedb::Result<()> {
let conf = "/path/to/curvine-cluster.toml";
let db = connect("curvine:///data/lancedb/demo")
    .storage_option(CURVINE_CONF_FILE_KEY, conf)
    .execute()
    .await?;

let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
let batch = RecordBatch::try_new(
    schema,
    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
)?;

let table = db
    .create_table("items", batch)
    .storage_option(CURVINE_CONF_FILE_KEY, conf)
    .execute()
    .await?;

let rows = table.query().execute().await?.try_collect::<Vec<_>>().await?;
assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
# Ok(())
# }
```

## Notes

- Use `curvine://` URIs. FUSE is not required.
- The workspace root is a Curvine directory, for example
  `curvine:///data/lancedb/demo`.
- This crate delegates non-Curvine LanceDB APIs to upstream LanceDB and adds the
  Curvine object store, session, and safe commit wiring needed for Curvine.
- Current validation includes the `curvine-tests` LanceDB object-store e2e suite
  against `10.209.148.124:8995,10.209.148.125:8995,10.209.148.127:8995`.
