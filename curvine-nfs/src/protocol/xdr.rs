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

//! XDR (External Data Representation) serialization
//!
//! Implementation of RFC 1014 XDR standard for NFS protocol.
//! See https://datatracker.ietf.org/doc/html/rfc1014

use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

/// XDR uses big-endian byte order
pub type XDREndian = BigEndian;

/// XDR serialization trait
#[allow(clippy::upper_case_acronyms)]
pub trait XDR {
    /// Serialize to XDR format
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()>;
    /// Deserialize from XDR format
    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()>;
}

/// Macro for serializing basic enumerations as u32 BigEndian
/// Uses num_enum's TryFrom implementation for deserialization
#[macro_export]
macro_rules! XDREnumSerde {
    ($t:ident) => {
        impl XDR for $t {
            fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
                dest.write_u32::<XDREndian>((*self).into())
            }
            fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
                let r: u32 = src.read_u32::<XDREndian>()?;
                match $t::try_from(r) {
                    Ok(p) => {
                        *self = p;
                        Ok(())
                    }
                    Err(_) => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid value {} for {}", r, stringify!($t)),
                    )),
                }
            }
        }
    };
}

/// Macro for serializing structs with named fields
#[macro_export]
macro_rules! XDRStruct {
    ($t:ident, $($element:ident),*) => {
        impl XDR for $t {
            fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
                $(self.$element.serialize(dest)?;)*
                Ok(())
            }
            fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
                $(self.$element.deserialize(src)?;)*
                Ok(())
            }
        }
    };
}

/// Macro for XDR unions with boolean discriminant
///
/// Handles unions of the form:
/// ```text
/// union pre_op_attr switch (bool attributes_follow) {
///     case TRUE:  wcc_attr attributes;
///     case FALSE: void;
/// };
/// ```
#[macro_export]
macro_rules! XDRBoolUnion {
    ($t:ident, $enumcase:ident, $enumtype:ty) => {
        impl XDR for $t {
            fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
                match self {
                    $t::Void => {
                        false.serialize(dest)?;
                    }
                    $t::$enumcase(v) => {
                        true.serialize(dest)?;
                        v.serialize(dest)?;
                    }
                }
                Ok(())
            }
            fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
                let mut c: bool = false;
                c.deserialize(src)?;
                if !c {
                    *self = $t::Void;
                } else {
                    let mut r = <$enumtype>::default();
                    r.deserialize(src)?;
                    *self = $t::$enumcase(r);
                }
                Ok(())
            }
        }
    };
}

// Re-export macros for use in other modules
pub use XDRBoolUnion;
pub use XDREnumSerde;
pub use XDRStruct;

// ============================================================================
// Primitive type implementations
// ============================================================================

impl XDR for bool {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        dest.write_u32::<XDREndian>(*self as u32)
    }
    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        *self = src.read_u32::<XDREndian>()? > 0;
        Ok(())
    }
}

impl XDR for i32 {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        dest.write_i32::<XDREndian>(*self)
    }
    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        *self = src.read_i32::<XDREndian>()?;
        Ok(())
    }
}

impl XDR for i64 {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        dest.write_i64::<XDREndian>(*self)
    }
    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        *self = src.read_i64::<XDREndian>()?;
        Ok(())
    }
}

impl XDR for u32 {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        dest.write_u32::<XDREndian>(*self)
    }
    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        *self = src.read_u32::<XDREndian>()?;
        Ok(())
    }
}

impl XDR for u64 {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        dest.write_u64::<XDREndian>(*self)
    }
    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        *self = src.read_u64::<XDREndian>()?;
        Ok(())
    }
}

impl<const N: usize> XDR for [u8; N] {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        dest.write_all(self)
    }
    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        src.read_exact(self)
    }
}

/// Maximum allowed size for XDR variable-length data (16MB)
/// This prevents malicious clients from causing OOM by sending huge length values
const XDR_MAX_DATA_SIZE: usize = 16 * 1024 * 1024;

impl XDR for Vec<u8> {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        if self.len() >= u32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Data too large for XDR serialization",
            ));
        }
        let length = self.len() as u32;
        length.serialize(dest)?;
        dest.write_all(self)?;
        // XDR requires 4-byte alignment padding
        let pad = ((4 - length % 4) % 4) as usize;
        if pad > 0 {
            dest.write_all(&[0u8; 4][..pad])?;
        }
        Ok(())
    }
    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        let mut length: u32 = 0;
        length.deserialize(src)?;

        // SECURITY: Limit maximum allocation size to prevent OOM attacks
        let length_usize = length as usize;
        if length_usize > XDR_MAX_DATA_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("XDR data size {length_usize} exceeds maximum allowed {XDR_MAX_DATA_SIZE}"),
            ));
        }

        self.resize(length_usize, 0);
        src.read_exact(self)?;
        // Read padding bytes
        let pad = ((4 - length % 4) % 4) as usize;
        if pad > 0 {
            let mut zeros = [0u8; 4];
            src.read_exact(&mut zeros[..pad])?;
        }
        Ok(())
    }
}

/// Maximum allowed size for XDR u32 array (1M elements)
const XDR_MAX_ARRAY_SIZE: usize = 1024 * 1024;

impl XDR for Vec<u32> {
    fn serialize<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        if self.len() >= u32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Array too large for XDR serialization",
            ));
        }
        (self.len() as u32).serialize(dest)?;
        for item in self {
            item.serialize(dest)?;
        }
        Ok(())
    }
    fn deserialize<R: Read>(&mut self, src: &mut R) -> std::io::Result<()> {
        let mut length: u32 = 0;
        length.deserialize(src)?;

        // SECURITY: Limit maximum array size to prevent OOM attacks
        let length_usize = length as usize;
        if length_usize > XDR_MAX_ARRAY_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "XDR array size {length_usize} exceeds maximum allowed {XDR_MAX_ARRAY_SIZE}"
                ),
            ));
        }

        self.resize(length_usize, 0);
        for item in self.iter_mut() {
            item.deserialize(src)?;
        }
        Ok(())
    }
}
