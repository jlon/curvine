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

//! Portmap Protocol data types (RFC 1057 Appendix A)

// This is a complete enumeration of everything in the RFC
#![allow(dead_code)]
// Keep the original RFC names and case
#![allow(non_camel_case_types)]

use crate::protocol::xdr::*;
use std::io::{Read, Write};

// Portmap protocol constants
pub const IPPROTO_TCP: u32 = 6; // Protocol number for TCP/IP
pub const IPPROTO_UDP: u32 = 17; // Protocol number for UDP/IP
pub const PROGRAM: u32 = 100000;
pub const VERSION: u32 = 2;

/// Port mapping entry
#[derive(Copy, Clone, Debug, Default)]
#[repr(C)]
pub struct mapping {
    pub prog: u32,
    pub vers: u32,
    pub prot: u32,
    pub port: u32,
}
XDRStruct!(mapping, prog, vers, prot, port);
