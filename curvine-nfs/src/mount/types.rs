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

//! Mount Protocol data types (RFC 1813 Appendix I)

#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_local_definitions)]

use crate::protocol::xdr::*;
use byteorder::{ReadBytesExt, WriteBytesExt};
use num_enum::{FromPrimitive, IntoPrimitive};
use std::io::{Read, Write};

// Mount protocol constants
pub const PROGRAM: u32 = 100005;
pub const VERSION: u32 = 3;

pub const MNTPATHLEN: u32 = 1024; // Maximum bytes in a path name
pub const MNTNAMLEN: u32 = 255; // Maximum bytes in a name
pub const FHSIZE3: u32 = 64; // Maximum bytes in a V3 file handle

pub type fhandle3 = Vec<u8>;
pub type dirpath = Vec<u8>;
pub type name = Vec<u8>;

/// Mount operation status codes
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum mountstat3 {
    #[default]
    MNT3_OK = 0, // No error
    MNT3ERR_PERM = 1,            // Not owner
    MNT3ERR_NOENT = 2,           // No such file or directory
    MNT3ERR_IO = 5,              // I/O error
    MNT3ERR_ACCES = 13,          // Permission denied
    MNT3ERR_NOTDIR = 20,         // Not a directory
    MNT3ERR_INVAL = 22,          // Invalid argument
    MNT3ERR_NAMETOOLONG = 63,    // Filename too long
    MNT3ERR_NOTSUPP = 10004,     // Operation not supported
    MNT3ERR_SERVERFAULT = 10006, // A failure on the server
}
XDREnumSerde!(mountstat3);
