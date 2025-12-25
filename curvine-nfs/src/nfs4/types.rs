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

//! NFSv4.1 Type Definitions (RFC 5661)
//!
//! This module defines all NFSv4.1 data types used in the protocol.

use std::fmt;

// ============================================================================
// Basic Types
// ============================================================================

/// 64-bit file identifier
pub type Fileid4 = u64;

/// 64-bit offset
pub type Offset4 = u64;

/// 32-bit count
pub type Count4 = u32;

/// 64-bit length
pub type Length4 = u64;

/// 32-bit mode (permissions)
pub type Mode4 = u32;

/// 32-bit bitmap
pub type Bitmap4 = Vec<u32>;

/// UTF-8 string
pub type Utf8String = Vec<u8>;

/// Component name (file/directory name)
pub type Component4 = Utf8String;

/// Path name
pub type Pathname4 = Vec<Component4>;

/// Opaque data
pub type Opaque = Vec<u8>;

// ============================================================================
// Verifier and Identifiers
// ============================================================================

/// 8-byte verifier for client identification
pub type Verifier4 = [u8; 8];

/// 16-byte session identifier
pub type Sessionid4 = [u8; 16];

/// 64-bit client identifier
pub type Clientid4 = u64;

/// 8-byte server verifier
pub type Serverid4 = [u8; 8];

// ============================================================================
// File Handle
// ============================================================================

/// NFSv4 file handle (variable length, max 128 bytes)
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct Nfs4FileHandle {
    pub data: Vec<u8>,
}

impl Nfs4FileHandle {
    pub const MAX_SIZE: usize = 128;

    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl fmt::Debug for Nfs4FileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FH({} bytes)", self.data.len())
    }
}

// ============================================================================
// Stateid - State Identifier
// ============================================================================

/// State identifier for tracking open/lock state
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Stateid4 {
    /// Sequence number (incremented on each state change)
    pub seqid: u32,
    /// Opaque identifier (12 bytes)
    pub other: [u8; 12],
}

impl Stateid4 {
    /// Special stateid: anonymous (no state)
    pub const ANONYMOUS: Self = Self {
        seqid: 0,
        other: [0; 12],
    };

    /// Special stateid: read bypass (for special reads)
    pub const READ_BYPASS: Self = Self {
        seqid: 0,
        other: [0xff; 12],
    };

    /// Special stateid: current stateid (use current state)
    pub const CURRENT: Self = Self {
        seqid: 1,
        other: [0; 12],
    };

    pub fn new(seqid: u32, other: [u8; 12]) -> Self {
        Self { seqid, other }
    }

    pub fn is_special(&self) -> bool {
        self == &Self::ANONYMOUS || self == &Self::READ_BYPASS || self == &Self::CURRENT
    }
}

impl fmt::Debug for Stateid4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Stateid(seq={}, other={:02x?})",
            self.seqid,
            &self.other[..4]
        )
    }
}

// ============================================================================
// Client Owner
// ============================================================================

/// Client owner for EXCHANGE_ID
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct ClientOwner4 {
    /// Client verifier (changes on client restart)
    pub co_verifier: Verifier4,
    /// Client owner ID (unique per client)
    pub co_ownerid: Vec<u8>,
}

impl fmt::Debug for ClientOwner4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientOwner(id={} bytes)", self.co_ownerid.len())
    }
}

// ============================================================================
// Lock Owner
// ============================================================================

/// Lock owner identifier
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct LockOwner4 {
    pub clientid: Clientid4,
    pub owner: Vec<u8>,
}

impl fmt::Debug for LockOwner4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LockOwner(client={}, owner={} bytes)",
            self.clientid,
            self.owner.len()
        )
    }
}

// ============================================================================
// Open Owner
// ============================================================================

/// Open owner identifier
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct OpenOwner4 {
    pub clientid: Clientid4,
    pub owner: Vec<u8>,
}

impl fmt::Debug for OpenOwner4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OpenOwner(client={}, owner={} bytes)",
            self.clientid,
            self.owner.len()
        )
    }
}

// ============================================================================
// Time
// ============================================================================

/// NFSv4 time (seconds + nanoseconds since epoch)
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct Nfstime4 {
    pub seconds: i64,
    pub nseconds: u32,
}

impl Nfstime4 {
    pub fn from_millis(ms: i64) -> Self {
        Self {
            seconds: ms / 1000,
            nseconds: ((ms % 1000) * 1_000_000) as u32,
        }
    }

    pub fn to_millis(&self) -> i64 {
        self.seconds * 1000 + (self.nseconds / 1_000_000) as i64
    }

    pub fn now() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            seconds: now.as_secs() as i64,
            nseconds: now.subsec_nanos(),
        }
    }
}

// ============================================================================
// File Type
// ============================================================================

/// NFSv4 file type
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum Nfs4FileType {
    #[default]
    Regular = 1,
    Directory = 2,
    Block = 3,
    Character = 4,
    Link = 5,
    Socket = 6,
    Fifo = 7,
    AttrDir = 8,
    NamedAttr = 9,
}

impl From<curvine_common::state::FileType> for Nfs4FileType {
    fn from(ft: curvine_common::state::FileType) -> Self {
        match ft {
            curvine_common::state::FileType::File => Self::Regular,
            curvine_common::state::FileType::Dir => Self::Directory,
            curvine_common::state::FileType::Link => Self::Link,
            _ => Self::Regular,
        }
    }
}

// ============================================================================
// File Attributes
// ============================================================================

/// NFSv4 file attributes
#[derive(Clone, Debug, Default)]
pub struct Fattr4 {
    pub attrmask: Bitmap4,
    pub attr_vals: Vec<u8>,
}

/// Decoded file attributes (for internal use)
#[derive(Clone, Debug, Default)]
pub struct FileAttrs {
    pub file_type: Nfs4FileType,
    pub mode: Mode4,
    pub nlink: u32,
    pub owner: String,
    pub group: String,
    pub size: u64,
    pub used: u64,
    pub fileid: Fileid4,
    pub atime: Nfstime4,
    pub mtime: Nfstime4,
    pub ctime: Nfstime4,
}

impl FileAttrs {
    pub fn from_status(status: &curvine_common::state::FileStatus) -> Self {
        Self {
            file_type: status.file_type.into(),
            mode: status.mode,
            nlink: status.nlink,
            owner: status.owner.clone(),
            group: status.group.clone(),
            size: status.len as u64,
            used: ((status.len + 511) / 512 * 512) as u64,
            fileid: status.id as u64,
            atime: Nfstime4::from_millis(status.atime),
            mtime: Nfstime4::from_millis(status.mtime),
            ctime: Nfstime4::from_millis(status.mtime),
        }
    }
}
