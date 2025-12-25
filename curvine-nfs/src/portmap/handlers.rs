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

//! Portmap Protocol request handlers

#![allow(clippy::upper_case_acronyms)]
#![allow(non_camel_case_types)]
#![allow(non_local_definitions)]

use crate::portmap;
use crate::protocol::rpc::*;
use crate::protocol::xdr::*;
use crate::server::context::RPCContext;
use num_enum::{FromPrimitive, IntoPrimitive};
use std::io::{Read, Write};
use tracing::{debug, error};

/*
 From RFC 1057 Appendix A

 program PMAP_PROG {
    version PMAP_VERS {
       void PMAPPROC_NULL(void)         = 0;
       bool PMAPPROC_SET(mapping)       = 1;
       bool PMAPPROC_UNSET(mapping)     = 2;
       unsigned int PMAPPROC_GETPORT(mapping)   = 3;
       pmaplist PMAPPROC_DUMP(void)         = 4;
       call_result PMAPPROC_CALLIT(call_args)  = 5;
    } = 2;
 } = 100000;
*/

#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
enum PortmapProgram {
    PMAPPROC_NULL = 0,
    PMAPPROC_SET = 1,
    PMAPPROC_UNSET = 2,
    PMAPPROC_GETPORT = 3,
    PMAPPROC_DUMP = 4,
    PMAPPROC_CALLIT = 5,
    #[default]
    INVALID = 6,
}

pub fn handle_portmap(
    xid: u32,
    call: call_body,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    if call.vers != portmap::VERSION {
        error!(
            "Invalid Portmap Version number {} != {}",
            call.vers,
            portmap::VERSION
        );
        prog_mismatch_reply_message(xid, portmap::VERSION).serialize(output)?;
        return Ok(());
    }
    let prog = PortmapProgram::from(call.proc);

    match prog {
        PortmapProgram::PMAPPROC_NULL => pmapproc_null(xid, input, output)?,
        PortmapProgram::PMAPPROC_GETPORT => pmapproc_getport(xid, input, output, context)?,
        _ => {
            proc_unavail_reply_message(xid).serialize(output)?;
        }
    }
    Ok(())
}

pub fn pmapproc_null(
    xid: u32,
    _: &mut impl Read,
    output: &mut impl Write,
) -> Result<(), anyhow::Error> {
    debug!("pmapproc_null({:?}) ", xid);
    let msg = make_success_reply(xid);
    debug!("\t{:?} --> {:?}", xid, msg);
    msg.serialize(output)?;
    Ok(())
}

/// Fake portmapper that always directs back to the same host port
pub fn pmapproc_getport(
    xid: u32,
    read: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut mapping = portmap::mapping::default();
    mapping.deserialize(read)?;
    debug!("pmapproc_getport({:?}, {:?}) ", xid, mapping);
    make_success_reply(xid).serialize(output)?;
    let port = context.local_port as u32;
    debug!("\t{:?} --> {:?}", xid, port);
    port.serialize(output)?;
    Ok(())
}
