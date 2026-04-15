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

//! RPC request context

use crate::nfs4::CompoundHandler;
use crate::protocol::rpc::auth_unix;
use crate::server::transaction::TransactionTracker;
use crate::vfs::NFSFileSystem;
use anyhow::Error;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::bytes::Bytes;

pub type OutboundTx = mpsc::UnboundedSender<Result<Bytes, Error>>;

/// Context for processing RPC requests
#[derive(Clone)]
pub struct RPCContext {
    /// Local port the server is listening on
    pub local_port: u16,
    /// Client address (Arc<str> to avoid repeated allocations)
    pub client_addr: Arc<str>,
    /// Unix authentication credentials from the request
    pub auth: auth_unix,
    /// Virtual filesystem implementation (NFSv3)
    pub vfs: Arc<dyn NFSFileSystem + Send + Sync>,
    /// NFSv4.1 COMPOUND handler
    pub nfs4_handler: Option<Arc<CompoundHandler>>,
    /// Channel to signal mount/unmount events
    pub mount_signal: Option<mpsc::Sender<bool>>,
    /// NFS export name (e.g., "/")
    pub export_name: Arc<String>,
    /// Transaction tracker for retransmission detection
    pub transaction_tracker: Arc<TransactionTracker>,
    /// Raw outbound channel for replies and server-initiated callbacks
    pub outbound_tx: Option<OutboundTx>,
}

impl fmt::Debug for RPCContext {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RPCContext")
            .field("local_port", &self.local_port)
            .field("client_addr", &self.client_addr)
            .field("auth", &self.auth)
            .field("nfs4_enabled", &self.nfs4_handler.is_some())
            .field("has_outbound_tx", &self.outbound_tx.is_some())
            .finish()
    }
}
