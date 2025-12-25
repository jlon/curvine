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

//! RPC Protocol definitions
//!
//! Implementation of RFC 1057 RPC protocol types.
//! See https://datatracker.ietf.org/doc/html/rfc1057

#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_local_definitions)]

use super::xdr::*;
use byteorder::{ReadBytesExt, WriteBytesExt};
use num_enum::{FromPrimitive, IntoPrimitive};
use std::io::{Read, Write};

/// Message type discriminant for rpc_body
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum _msg_type {
    #[default]
    CALL = 0,
    REPLY = 1,
}
XDREnumSerde!(_msg_type);

/// Reply status discriminant
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum _reply_stat {
    #[default]
    MSG_ACCEPTED = 0,
    MSG_DENIED = 1,
}
XDREnumSerde!(_reply_stat);

/// Accept status for accepted replies
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum _accept_stat {
    #[default]
    SUCCESS = 0,
    PROG_UNAVAIL = 1,
    PROG_MISMATCH = 2,
    PROC_UNAVAIL = 3,
    GARBAGE_ARGS = 4,
}
XDREnumSerde!(_accept_stat);

/// Reject status for rejected replies
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum _reject_stat {
    #[default]
    RPC_MISMATCH = 0,
    AUTH_ERROR = 1,
}
XDREnumSerde!(_reject_stat);

/// Authentication failure reasons
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum auth_stat {
    #[default]
    AUTH_BADCRED = 1,
    AUTH_REJECTEDCRED = 2,
    AUTH_BADVERF = 3,
    AUTH_REJECTEDVERF = 4,
    AUTH_TOOWEAK = 5,
}
XDREnumSerde!(auth_stat);

/// Authentication flavor
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum auth_flavor {
    #[default]
    AUTH_NULL = 0,
    AUTH_UNIX = 1,
    AUTH_SHORT = 2,
    AUTH_DES = 3,
}
XDREnumSerde!(auth_flavor);

// ============================================================================
// Authentication structures
// ============================================================================

/// Unix authentication credentials
#[derive(Clone, Debug, Default)]
pub struct auth_unix {
    pub stamp: u32,
    pub machinename: Vec<u8>,
    pub uid: u32,
    pub gid: u32,
    pub gids: Vec<u32>,
}
XDRStruct!(auth_unix, stamp, machinename, uid, gid, gids);

/// Opaque authentication data
#[derive(Clone, Debug)]
pub struct opaque_auth {
    pub flavor: auth_flavor,
    pub body: Vec<u8>,
}
XDRStruct!(opaque_auth, flavor, body);

impl Default for opaque_auth {
    fn default() -> Self {
        Self {
            flavor: auth_flavor::AUTH_NULL,
            body: Vec::new(),
        }
    }
}

// ============================================================================
// RPC Message structures
// ============================================================================

/// RPC message (call or reply)
#[derive(Clone, Debug, Default)]
pub struct rpc_msg {
    pub xid: u32,
    pub body: rpc_body,
}
XDRStruct!(rpc_msg, xid, body);

/// RPC message body (discriminated union)
#[derive(Clone, Debug)]
#[repr(u32)]
pub enum rpc_body {
    CALL(call_body),
    REPLY(reply_body),
}

impl Default for rpc_body {
    fn default() -> Self {
        rpc_body::CALL(call_body::default())
    }
}

impl XDR for rpc_body {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        match self {
            rpc_body::CALL(v) => {
                0_u32.serialize(dest)?;
                v.serialize(dest)?;
            }
            rpc_body::REPLY(v) => {
                1_u32.serialize(dest)?;
                v.serialize(dest)?;
            }
        }
        Ok(())
    }

    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        let c = src.read_u32::<XDREndian>()?;
        match c {
            0 => {
                let mut r = call_body::default();
                r.deserialize(src)?;
                *self = rpc_body::CALL(r);
            }
            1 => {
                let mut r = reply_body::default();
                r.deserialize(src)?;
                *self = rpc_body::REPLY(r);
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid rpc_body discriminant",
                ));
            }
        }
        Ok(())
    }
}

/// RPC call body
#[derive(Clone, Debug, Default)]
pub struct call_body {
    pub rpcvers: u32, // Must be 2
    pub prog: u32,
    pub vers: u32,
    pub proc: u32,
    pub cred: opaque_auth,
    pub verf: opaque_auth,
}
XDRStruct!(call_body, rpcvers, prog, vers, proc, cred, verf);

/// RPC reply body (discriminated union)
#[derive(Clone, Debug)]
#[repr(u32)]
pub enum reply_body {
    MSG_ACCEPTED(accepted_reply),
    MSG_DENIED(rejected_reply),
}

impl Default for reply_body {
    fn default() -> Self {
        reply_body::MSG_ACCEPTED(accepted_reply::default())
    }
}

impl XDR for reply_body {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        match self {
            reply_body::MSG_ACCEPTED(v) => {
                0_u32.serialize(dest)?;
                v.serialize(dest)?;
            }
            reply_body::MSG_DENIED(v) => {
                1_u32.serialize(dest)?;
                v.serialize(dest)?;
            }
        }
        Ok(())
    }

    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        let c = src.read_u32::<XDREndian>()?;
        match c {
            0 => {
                let mut r = accepted_reply::default();
                r.deserialize(src)?;
                *self = reply_body::MSG_ACCEPTED(r);
            }
            1 => {
                let mut r = rejected_reply::default();
                r.deserialize(src)?;
                *self = reply_body::MSG_DENIED(r);
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid reply_body discriminant",
                ));
            }
        }
        Ok(())
    }
}

// ============================================================================
// Reply structures
// ============================================================================

/// Version mismatch information
#[derive(Copy, Clone, Debug, Default)]
#[repr(C)]
pub struct mismatch_info {
    pub low: u32,
    pub high: u32,
}
XDRStruct!(mismatch_info, low, high);

/// Accepted reply
#[derive(Clone, Debug, Default)]
pub struct accepted_reply {
    pub verf: opaque_auth,
    pub reply_data: accept_body,
}
XDRStruct!(accepted_reply, verf, reply_data);

/// Accept body (discriminated union)
#[derive(Copy, Clone, Debug, Default)]
#[repr(u32)]
pub enum accept_body {
    #[default]
    SUCCESS,
    PROG_UNAVAIL,
    PROG_MISMATCH(mismatch_info),
    PROC_UNAVAIL,
    GARBAGE_ARGS,
}

impl XDR for accept_body {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        match self {
            accept_body::SUCCESS => 0_u32.serialize(dest),
            accept_body::PROG_UNAVAIL => 1_u32.serialize(dest),
            accept_body::PROG_MISMATCH(v) => {
                2_u32.serialize(dest)?;
                v.serialize(dest)
            }
            accept_body::PROC_UNAVAIL => 3_u32.serialize(dest),
            accept_body::GARBAGE_ARGS => 4_u32.serialize(dest),
        }
    }

    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        let c = src.read_u32::<XDREndian>()?;
        *self = match c {
            0 => accept_body::SUCCESS,
            1 => accept_body::PROG_UNAVAIL,
            2 => {
                let mut r = mismatch_info::default();
                r.deserialize(src)?;
                accept_body::PROG_MISMATCH(r)
            }
            3 => accept_body::PROC_UNAVAIL,
            _ => accept_body::GARBAGE_ARGS,
        };
        Ok(())
    }
}

/// Rejected reply (discriminated union)
#[derive(Clone, Debug)]
#[repr(u32)]
pub enum rejected_reply {
    RPC_MISMATCH(mismatch_info),
    AUTH_ERROR(auth_stat),
}

impl Default for rejected_reply {
    fn default() -> Self {
        rejected_reply::RPC_MISMATCH(mismatch_info::default())
    }
}

impl XDR for rejected_reply {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        match self {
            rejected_reply::RPC_MISMATCH(v) => {
                0_u32.serialize(dest)?;
                v.serialize(dest)?;
            }
            rejected_reply::AUTH_ERROR(v) => {
                1_u32.serialize(dest)?;
                v.serialize(dest)?;
            }
        }
        Ok(())
    }

    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        let c = src.read_u32::<XDREndian>()?;
        match c {
            0 => {
                let mut r = mismatch_info::default();
                r.deserialize(src)?;
                *self = rejected_reply::RPC_MISMATCH(r);
            }
            _ => {
                let mut r = auth_stat::default();
                r.deserialize(src)?;
                *self = rejected_reply::AUTH_ERROR(r);
            }
        }
        Ok(())
    }
}

// ============================================================================
// Helper functions for creating reply messages
// ============================================================================

/// Create a success reply message
#[inline]
pub fn make_success_reply(xid: u32) -> rpc_msg {
    rpc_msg {
        xid,
        body: rpc_body::REPLY(reply_body::MSG_ACCEPTED(accepted_reply {
            verf: opaque_auth::default(),
            reply_data: accept_body::SUCCESS,
        })),
    }
}

/// Create a procedure unavailable reply
#[inline]
pub fn proc_unavail_reply_message(xid: u32) -> rpc_msg {
    rpc_msg {
        xid,
        body: rpc_body::REPLY(reply_body::MSG_ACCEPTED(accepted_reply {
            verf: opaque_auth::default(),
            reply_data: accept_body::PROC_UNAVAIL,
        })),
    }
}

/// Create a program unavailable reply
#[inline]
pub fn prog_unavail_reply_message(xid: u32) -> rpc_msg {
    rpc_msg {
        xid,
        body: rpc_body::REPLY(reply_body::MSG_ACCEPTED(accepted_reply {
            verf: opaque_auth::default(),
            reply_data: accept_body::PROG_UNAVAIL,
        })),
    }
}

/// Create a program mismatch reply
#[inline]
pub fn prog_mismatch_reply_message(xid: u32, accepted_ver: u32) -> rpc_msg {
    rpc_msg {
        xid,
        body: rpc_body::REPLY(reply_body::MSG_ACCEPTED(accepted_reply {
            verf: opaque_auth::default(),
            reply_data: accept_body::PROG_MISMATCH(mismatch_info {
                low: accepted_ver,
                high: accepted_ver,
            }),
        })),
    }
}

/// Create a garbage arguments reply
#[inline]
pub fn garbage_args_reply_message(xid: u32) -> rpc_msg {
    rpc_msg {
        xid,
        body: rpc_body::REPLY(reply_body::MSG_ACCEPTED(accepted_reply {
            verf: opaque_auth::default(),
            reply_data: accept_body::GARBAGE_ARGS,
        })),
    }
}

/// Create an RPC version mismatch reply
#[inline]
pub fn rpc_vers_mismatch(xid: u32) -> rpc_msg {
    rpc_msg {
        xid,
        body: rpc_body::REPLY(reply_body::MSG_DENIED(rejected_reply::RPC_MISMATCH(
            mismatch_info::default(),
        ))),
    }
}
