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

//! NFSv4.1 XDR Encoding/Decoding
//!
//! Extends the base XDR module with NFSv4.1 specific types.

use crate::nfs4::error::Nfs4Status;
use crate::nfs4::types::*;
use crate::protocol::xdr::XDR;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Read, Write};

// ============================================================================
// Nfs4FileHandle XDR
// ============================================================================

impl XDR for Nfs4FileHandle {
    fn serialize<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        (self.data.len() as u32).serialize(output)?;
        output.write_all(&self.data)?;
        // XDR 4-byte alignment
        let pad = (4 - self.data.len() % 4) % 4;
        if pad > 0 {
            output.write_all(&[0u8; 4][..pad])?;
        }
        Ok(())
    }

    fn deserialize<R: Read>(&mut self, input: &mut R) -> std::io::Result<()> {
        let len = input.read_u32::<BigEndian>()? as usize;
        if len > Self::MAX_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("File handle too large: {}", len),
            ));
        }
        self.data.resize(len, 0);
        input.read_exact(&mut self.data)?;
        // Skip padding
        let pad = (4 - len % 4) % 4;
        if pad > 0 {
            let mut skip = [0u8; 4];
            input.read_exact(&mut skip[..pad])?;
        }
        Ok(())
    }
}

// ============================================================================
// Stateid4 XDR
// ============================================================================

impl XDR for Stateid4 {
    fn serialize<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        self.seqid.serialize(output)?;
        output.write_all(&self.other)?;
        Ok(())
    }

    fn deserialize<R: Read>(&mut self, input: &mut R) -> std::io::Result<()> {
        self.seqid.deserialize(input)?;
        input.read_exact(&mut self.other)?;
        Ok(())
    }
}

// ============================================================================
// ClientOwner4 XDR
// ============================================================================

impl XDR for ClientOwner4 {
    fn serialize<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        output.write_all(&self.co_verifier)?;
        self.co_ownerid.serialize(output)?;
        Ok(())
    }

    fn deserialize<R: Read>(&mut self, input: &mut R) -> std::io::Result<()> {
        input.read_exact(&mut self.co_verifier)?;
        self.co_ownerid.deserialize(input)?;
        Ok(())
    }
}

// ============================================================================
// LockOwner4 XDR
// ============================================================================

impl XDR for LockOwner4 {
    fn serialize<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        self.clientid.serialize(output)?;
        self.owner.serialize(output)?;
        Ok(())
    }

    fn deserialize<R: Read>(&mut self, input: &mut R) -> std::io::Result<()> {
        self.clientid.deserialize(input)?;
        self.owner.deserialize(input)?;
        Ok(())
    }
}

// ============================================================================
// OpenOwner4 XDR
// ============================================================================

impl XDR for OpenOwner4 {
    fn serialize<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        self.clientid.serialize(output)?;
        self.owner.serialize(output)?;
        Ok(())
    }

    fn deserialize<R: Read>(&mut self, input: &mut R) -> std::io::Result<()> {
        self.clientid.deserialize(input)?;
        self.owner.deserialize(input)?;
        Ok(())
    }
}

// ============================================================================
// Nfstime4 XDR
// ============================================================================

impl XDR for Nfstime4 {
    fn serialize<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        self.seconds.serialize(output)?;
        self.nseconds.serialize(output)?;
        Ok(())
    }

    fn deserialize<R: Read>(&mut self, input: &mut R) -> std::io::Result<()> {
        self.seconds.deserialize(input)?;
        self.nseconds.deserialize(input)?;
        Ok(())
    }
}

// ============================================================================
// Nfs4Status XDR
// ============================================================================

impl XDR for Nfs4Status {
    fn serialize<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        (*self as u32).serialize(output)
    }

    fn deserialize<R: Read>(&mut self, input: &mut R) -> std::io::Result<()> {
        let val = input.read_u32::<BigEndian>()?;
        *self = Nfs4Status::from(val);
        Ok(())
    }
}

// ============================================================================
// Nfs4FileType XDR
// ============================================================================

impl XDR for Nfs4FileType {
    fn serialize<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        (*self as u32).serialize(output)
    }

    fn deserialize<R: Read>(&mut self, input: &mut R) -> std::io::Result<()> {
        let val = input.read_u32::<BigEndian>()?;
        *self = match val {
            1 => Nfs4FileType::Regular,
            2 => Nfs4FileType::Directory,
            3 => Nfs4FileType::Block,
            4 => Nfs4FileType::Character,
            5 => Nfs4FileType::Link,
            6 => Nfs4FileType::Socket,
            7 => Nfs4FileType::Fifo,
            8 => Nfs4FileType::AttrDir,
            9 => Nfs4FileType::NamedAttr,
            _ => Nfs4FileType::Regular,
        };
        Ok(())
    }
}

// ============================================================================
// Fattr4 XDR
// ============================================================================

impl XDR for Fattr4 {
    fn serialize<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        self.attrmask.serialize(output)?;
        self.attr_vals.serialize(output)?;
        Ok(())
    }

    fn deserialize<R: Read>(&mut self, input: &mut R) -> std::io::Result<()> {
        self.attrmask.deserialize(input)?;
        self.attr_vals.deserialize(input)?;
        Ok(())
    }
}
