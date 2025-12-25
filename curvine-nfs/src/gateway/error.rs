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

//! NFS Gateway error types
//!
//! This module provides error handling for NFS operations, following the same
//! pattern as curvine-fuse/src/fuse_error.rs for consistency.

use crate::nfs::nfsstat3;
use curvine_common::error::FsError;
use std::fmt;

/// NFS Gateway error type
/// Wraps nfsstat3 error code with detailed error message
#[derive(Debug)]
pub struct NfsError {
    stat: nfsstat3,
    message: String,
}

impl NfsError {
    /// Create a new NFS error with status code and message
    #[inline]
    pub fn new(stat: nfsstat3, message: impl Into<String>) -> Self {
        Self {
            stat,
            message: message.into(),
        }
    }

    /// Get the NFS status code
    #[inline]
    pub fn stat(&self) -> nfsstat3 {
        self.stat
    }

    /// Create an IO error
    #[inline]
    pub fn io(message: impl Into<String>) -> Self {
        Self::new(nfsstat3::NFS3ERR_IO, message)
    }

    /// Create a not found error
    #[inline]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(nfsstat3::NFS3ERR_NOENT, message)
    }

    /// Create a stale handle error
    #[inline]
    pub fn stale(message: impl Into<String>) -> Self {
        Self::new(nfsstat3::NFS3ERR_STALE, message)
    }

    /// Create an invalid argument error
    #[inline]
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(nfsstat3::NFS3ERR_INVAL, message)
    }
}

impl std::error::Error for NfsError {}

impl fmt::Display for NfsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NFS error {:?}: {}", self.stat, self.message)
    }
}

impl From<String> for NfsError {
    fn from(value: String) -> Self {
        Self::new(nfsstat3::NFS3ERR_IO, value)
    }
}

impl From<&str> for NfsError {
    fn from(value: &str) -> Self {
        Self::new(nfsstat3::NFS3ERR_IO, value)
    }
}

impl From<std::io::Error> for NfsError {
    fn from(value: std::io::Error) -> Self {
        Self::new(nfsstat3::NFS3ERR_IO, value.to_string())
    }
}

impl From<NfsError> for nfsstat3 {
    fn from(value: NfsError) -> Self {
        value.stat
    }
}

/// Convert FsError to NfsError with proper status code mapping
/// Pattern follows curvine-fuse/src/fuse_error.rs
impl From<FsError> for NfsError {
    fn from(value: FsError) -> Self {
        // Map well-known FsError kinds directly to NFS status codes
        let stat = match &value {
            FsError::FileAlreadyExists(_) => nfsstat3::NFS3ERR_EXIST,
            FsError::FileNotFound(_) => nfsstat3::NFS3ERR_NOENT,
            FsError::DirNotEmpty(_) => nfsstat3::NFS3ERR_NOTEMPTY,
            FsError::ParentNotDir(_) => nfsstat3::NFS3ERR_NOTDIR,
            FsError::InvalidPath(_) => nfsstat3::NFS3ERR_INVAL,
            FsError::DiskOutOfSpace(_) => nfsstat3::NFS3ERR_NOSPC,
            FsError::Timeout(_) => nfsstat3::NFS3ERR_JUKEBOX,
            FsError::Unsupported(_) => nfsstat3::NFS3ERR_NOTSUPP,
            FsError::InProgress(_) => nfsstat3::NFS3ERR_JUKEBOX,
            FsError::IO(_) => nfsstat3::NFS3ERR_IO,
            FsError::Expired(_) => nfsstat3::NFS3ERR_STALE,
            _ => {
                // Fallback: infer from message content
                let msg = value.to_string().to_lowercase();
                if msg.contains("permission denied") || msg.contains("os error 13") {
                    nfsstat3::NFS3ERR_ACCES
                } else if msg.contains("read only") || msg.contains("read-only") {
                    nfsstat3::NFS3ERR_ROFS
                } else if msg.contains("name too long") {
                    nfsstat3::NFS3ERR_NAMETOOLONG
                } else if msg.contains("is a directory") {
                    nfsstat3::NFS3ERR_ISDIR
                } else {
                    nfsstat3::NFS3ERR_SERVERFAULT
                }
            }
        };

        Self::new(stat, value.to_string())
    }
}

/// Result type alias for NFS operations
pub type NfsResult<T> = Result<T, NfsError>;
