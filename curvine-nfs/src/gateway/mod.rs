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

//! Curvine NFS Gateway implementation
//!
//! This module provides an NFSv3 gateway that exposes Curvine's UnifiedFileSystem
//! through the standard NFS protocol.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
//! │   NFS Client    │────▶│  NFS Gateway    │────▶│    Curvine      │
//! │   (OS kernel)   │     │  (nfsserve)     │     │    Cluster      │
//! └─────────────────┘     └─────────────────┘     └─────────────────┘
//! ```
//!
//! # Design Philosophy
//!
//! - FileBlocks cache: Avoids get_block_locations RPC on every read
//! - Writer cache: Enables UNSTABLE writes with deferred COMMIT
//! - PathCache: fileid -> path reverse lookup
//!
//! # Features
//!
//! - Direct use of Curvine's inode ID as NFS fileid
//! - I/O caching for read/write optimization
//! - UID/GID mapping using system calls
//! - Fixed cluster_generation for multi-instance deployment

mod curvine_nfs_fs;
mod error;
mod io_cache;
mod nfs_reader;
mod nfs_writer;
mod path_cache;
mod server;
mod uid_gid;

pub use curvine_nfs_fs::CurvineNfsFileSystem;
pub use error::{NfsError, NfsResult};
pub use io_cache::{IoCache, IoCacheConfig, ReaderPool, ReaderEntry};
pub use nfs_reader::NfsReader;
pub use nfs_writer::NfsWriter;
pub use server::NfsGatewayServer;
