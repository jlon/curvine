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

use crate::nfs4::compound::{CompoundContext, CompoundHandler};
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;

/// OPEN_CONFIRM operation handler (NFSv4.0 only)
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_open_confirm() at line 56
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized OPEN_CONFIRM4res
pub async fn op_open_confirm(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    if ctx.minor_version > 0 {
        return Err(Nfs4Status::Notsupp.into());
    }

    let mut stateid = Stateid4::default();
    stateid.deserialize(input)?;
    let _seqid = input.read_u32::<BigEndian>()?;

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    let open_state = handler
        .opens
        .get_state(&stateid)
        .ok_or(Nfs4Status::BadStateid)?;

    if open_state.fileid != fileid {
        return Err(Nfs4Status::BadStateid.into());
    }

    if open_state.is_confirmed() {
        return Err(Nfs4Status::BadStateid.into());
    }

    open_state.set_confirmed(true);
    let new_seqid = open_state.next_seqid();
    let confirmed_stateid = Stateid4::new(new_seqid, stateid.other);

    let mut result = Vec::new();
    confirmed_stateid.serialize(&mut result)?;

    Ok(result)
}
