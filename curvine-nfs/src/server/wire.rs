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

//! RPC wire protocol handling (RFC 5531)
//!
//! Optimized implementation:
//! - Direct TcpStream read via BufReader (no DuplexStream intermediate)
//! - Pre-allocated buffers to reduce allocations
//! - Uses Bytes for zero-copy response handling

use anyhow::anyhow;
use std::io::Cursor;
use std::io::{Read, Write};
use tokio_util::bytes::{BufMut, Bytes, BytesMut};
use tracing::{debug, error, trace, warn};

use crate::mount;
use crate::mount::handlers as mount_handlers;
use crate::nfs;
use crate::nfs::handlers as nfs_handlers;
use crate::portmap;
use crate::portmap::handlers as portmap_handlers;
use crate::protocol::rpc::*;
use crate::protocol::xdr::*;
use crate::server::context::RPCContext;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;

const NFS_ACL_PROGRAM: u32 = 100227;
const NFS_ID_MAP_PROGRAM: u32 = 100270;
const NFS_METADATA_PROGRAM: u32 = 200024;

/// Maximum RPC fragment size (128MB)
const MAX_FRAGMENT_SIZE: usize = 128 * 1024 * 1024;

/// Pre-allocated buffer capacity (256KB for high-throughput NFS operations)
/// Increased from 64KB to reduce reallocation overhead for large READ/WRITE
const BUF_CAPACITY: usize = 256 * 1024;

/// Handle an RPC message and write response to output buffer
async fn handle_rpc(
    input: &mut impl Read,
    output: &mut impl Write,
    mut context: RPCContext,
) -> Result<bool, anyhow::Error> {
    let mut recv = rpc_msg::default();
    recv.deserialize(input)?;
    let xid = recv.xid;

    let rpc_body::CALL(call) = recv.body else {
        error!("Unexpectedly received a Reply instead of a Call");
        return Err(anyhow!("Bad RPC Call format"));
    };

    // Parse AUTH_UNIX credentials if present
    if let auth_flavor::AUTH_UNIX = call.cred.flavor {
        let mut auth = auth_unix::default();
        auth.deserialize(&mut Cursor::new(&call.cred.body))?;
        context.auth = auth;
    } else {
        warn!(
            "RPC auth flavor: {:?}, using default (uid=0, gid=0)",
            call.cred.flavor
        );
    }

    // Validate RPC version
    if call.rpcvers != 2 {
        warn!("Invalid RPC version {} != 2", call.rpcvers);
        rpc_vers_mismatch(xid).serialize(output)?;
        return Ok(true);
    }

    // Check for retransmission
    if context
        .transaction_tracker
        .is_retransmission(xid, &context.client_addr)
    {
        debug!(
            "Retransmission detected, xid: {}, client: {}",
            xid, &context.client_addr
        );
        return Ok(false);
    }

    // Dispatch to appropriate handler
    let res = dispatch_rpc(xid, call, input, output, &context).await;
    context
        .transaction_tracker
        .mark_processed(xid, &context.client_addr);
    res.map(|_| true)
}

/// Dispatch RPC call to the appropriate program handler
async fn dispatch_rpc(
    xid: u32,
    call: call_body,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    match call.prog {
        p if p == nfs::PROGRAM => {
            // Check NFS version: 3 = NFSv3, 4 = NFSv4.1
            if call.vers == 4 {
                // NFSv4.1 handler
                crate::nfs4::handlers::handle_nfs4(xid, call, input, output, context).await
            } else {
                // NFSv3 handler
                nfs_handlers::handle_nfs(xid, call, input, output, context).await
            }
        }
        p if p == portmap::PROGRAM => {
            portmap_handlers::handle_portmap(xid, call, input, output, context)
        }
        p if p == mount::PROGRAM => {
            mount_handlers::handle_mount(xid, call, input, output, context).await
        }
        NFS_ACL_PROGRAM | NFS_ID_MAP_PROGRAM | NFS_METADATA_PROGRAM => {
            trace!("Ignoring NFS_ACL/ID_MAP/METADATA packet");
            prog_unavail_reply_message(xid).serialize(output)?;
            Ok(())
        }
        _ => {
            warn!("Unknown RPC Program number {}", call.prog);
            prog_unavail_reply_message(xid).serialize(output)?;
            Ok(())
        }
    }
}

/// RFC 1057 Section 10 - Record Marking
///
/// Read a fragment directly into the buffer.
/// Returns true if this is the last fragment.
///
/// # Performance Optimization
/// Uses unsafe to avoid zero-initialization overhead when resizing buffer.
/// The memory will be immediately overwritten by read_exact, so zeroing is wasteful.
async fn read_fragment(
    reader: &mut BufReader<OwnedReadHalf>,
    buf: &mut BytesMut,
) -> Result<bool, anyhow::Error> {
    // Read 4-byte header
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    let header_val = u32::from_be_bytes(header);
    let is_last = (header_val & (1 << 31)) != 0;
    let length = (header_val & 0x7FFF_FFFF) as usize;

    // Security check
    if length > MAX_FRAGMENT_SIZE {
        return Err(anyhow!("Fragment size {length} exceeds limit"));
    }
    let new_len = buf.len().saturating_add(length);
    if new_len > MAX_FRAGMENT_SIZE {
        return Err(anyhow!("Total message size {new_len} exceeds limit"));
    }

    trace!("Reading fragment: {} bytes, last={}", length, is_last);

    // Reserve space and read directly
    buf.reserve(length);
    let start = buf.len();

    // SAFETY: We're about to overwrite this memory with read_exact,
    // so zero-initialization is unnecessary overhead.
    // This is safe because:
    // 1. We reserved enough capacity above
    // 2. read_exact will fill the entire range [start..new_len]
    // 3. If read_exact fails, we return error and buf is dropped
    unsafe {
        buf.set_len(new_len);
    }

    reader.read_exact(&mut buf[start..]).await?;

    Ok(is_last)
}

/// Write a fragment with record marking header
#[inline]
pub async fn write_fragment(writer: &mut OwnedWriteHalf, data: &[u8]) -> Result<(), anyhow::Error> {
    if data.len() >= 0x8000_0000 {
        return Err(anyhow!("Fragment too large: {}", data.len()));
    }
    // Set last-fragment flag (bit 31)
    let header = (data.len() as u32) | 0x8000_0000;
    writer.write_all(&header.to_be_bytes()).await?;
    writer.write_all(data).await?;
    trace!("Wrote fragment: {} bytes", data.len());
    Ok(())
}

/// Response message type using Bytes for zero-copy
pub type SocketMessageType = Result<Bytes, anyhow::Error>;

/// Socket message handler - reads from TcpStream directly
pub struct SocketMessageHandler {
    buf: BytesMut,
    reader: BufReader<OwnedReadHalf>,
    reply_tx: mpsc::UnboundedSender<SocketMessageType>,
    context: RPCContext,
}

impl std::fmt::Debug for SocketMessageHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketMessageHandler")
            .field("buf_len", &self.buf.len())
            .field("context", &self.context)
            .finish()
    }
}

impl SocketMessageHandler {
    /// Create handler with read half of TcpStream
    pub fn new(
        read_half: OwnedReadHalf,
        context: &RPCContext,
    ) -> (Self, mpsc::UnboundedReceiver<SocketMessageType>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                buf: BytesMut::with_capacity(BUF_CAPACITY),
                reader: BufReader::with_capacity(BUF_CAPACITY, read_half),
                reply_tx: tx,
                context: context.clone(),
            },
            rx,
        )
    }

    /// Read and process one complete RPC message
    ///
    /// # Performance Optimization
    /// Avoids BytesMut→Cursor copy by using Bytes directly as Read source
    pub async fn read(&mut self) -> Result<(), anyhow::Error> {
        // Read fragments until last one
        loop {
            let is_last = read_fragment(&mut self.reader, &mut self.buf).await?;
            if is_last {
                break;
            }
        }

        // Take the complete message (zero-copy split)
        let request = self.buf.split().freeze();
        // Pre-allocate for next message
        self.buf.reserve(BUF_CAPACITY);

        // Spawn handler task
        let context = self.context.clone();
        let reply_tx = self.reply_tx.clone();

        tokio::spawn(async move {
            // Use BytesMut for response - avoids Vec→Bytes conversion
            let mut response = BytesMut::with_capacity(BUF_CAPACITY);

            // OPTIMIZATION: Use Bytes directly as Read source (implements std::io::Read via Cursor)
            // This avoids the BytesMut→Cursor copy
            let mut cursor = Cursor::new(&request[..]);
            let mut writer = (&mut response).writer();
            let result = handle_rpc(&mut cursor, &mut writer, context).await;

            match result {
                Ok(true) => {
                    // Zero-copy freeze: BytesMut → Bytes
                    let _ = reply_tx.send(Ok(response.freeze()));
                }
                Ok(false) => {
                    // Retransmission - no reply
                }
                Err(e) => {
                    error!("RPC error: {:?}", e);
                    let _ = reply_tx.send(Err(e));
                }
            }
        });

        Ok(())
    }
}
