// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! NFSv4 Operation Handlers
//!
//! This module is organized following NFS-Ganesha's structure:
//! - Each major operation has its own file (e.g., open.rs, close.rs)
//! - Mirrors nfs-ganesha/src/Protocols/NFS/nfs4_op_*.c files
//!
//! # Architecture Alignment
//!
//! NFS-Ganesha structure:
//! ```text
//! nfs-ganesha/src/Protocols/NFS/
//! ├── nfs4_op_open.c    (1648 lines)
//! ├── nfs4_op_close.c   (300+ lines)
//! ├── nfs4_op_read.c
//! ├── nfs4_op_write.c
//! └── ...
//! ```
//!
//! Our structure:
//! ```text
//! curvine-nfs/src/nfs4/ops/
//! ├── open.rs     (mirrors nfs4_op_open.c)
//! ├── close.rs    (mirrors nfs4_op_close.c)
//! ├── read.rs     (mirrors nfs4_op_read.c)
//! ├── write.rs    (mirrors nfs4_op_write.c)
//! └── ...
//! ```

pub mod access;
pub mod close;
pub mod commit;
pub mod create;
pub mod getattr;
pub mod lookup;
pub mod open;
pub mod open_confirm;
pub mod open_downgrade;
pub mod openattr;
pub mod read;
pub mod readdir;
pub mod remove;
pub mod rename;
pub mod setattr;
pub mod write;

// Re-export main operation handlers
pub use access::op_access;
pub use close::op_close;
pub use commit::op_commit;
pub use create::op_create;
pub use getattr::op_getattr;
pub use lookup::op_lookup;
pub use open::op_open;
pub use open_confirm::op_open_confirm;
pub use open_downgrade::op_open_downgrade;
pub use openattr::op_openattr;
pub use read::op_read;
pub use readdir::op_readdir;
pub use remove::op_remove;
pub use rename::op_rename;
pub use setattr::op_setattr;
pub use write::op_write;
