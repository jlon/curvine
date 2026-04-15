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

use crate::nfs4::error::{Nfs4Error, Nfs4Result, Nfs4Status};
use crate::nfs4::types::{FileAttrs, Fileid4, Nfs4FileHandle, Nfs4FileType, Nfstime4, Stateid4};
use crate::protocol::xdr::XDR;
use curvine_client::block::{BlockReaderLocal, BlockReaderRemote};
use curvine_client::file::FsContext;
use curvine_common::state::{ExtendedBlock, FileBlocks, FileType, StorageType, WorkerAddress};
use hmac::{Hmac, Mac};
use orpc::sys::DataSlice;
use sha2::Sha256;
use std::collections::HashMap;
use std::io::Write;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

pub const LAYOUT4_NFSV4_1_FILES: u32 = 1;
pub const LAYOUTIOMODE4_READ: u32 = 1;
pub const LAYOUTIOMODE4_RW: u32 = 2;
pub const LAYOUTRETURN4_FILE: u32 = 1;
const NFL4_UFLG_DENSE: u32 = 0x0000_0001;
const NFL4_UFLG_STRIPE_UNIT_SIZE_MASK: u32 = 0xFFFF_FFC0;
const PNFS_BLOCK_FH_MAGIC: u32 = 0x5044_5331;
const PNFS_BLOCK_FH_TAG_LEN: usize = 16;

#[derive(Clone, Debug)]
pub struct LayoutState {
    pub stateid: Stateid4,
    pub clientid: u64,
    pub fileid: Fileid4,
    pub deviceid: [u8; 16],
}

#[derive(Clone)]
struct DeviceState {
    blocks: FileBlocks,
    refs: usize,
}

pub struct PnfsManager {
    layouts: RwLock<HashMap<[u8; 12], LayoutState>>,
    devices: RwLock<HashMap<[u8; 16], DeviceState>>,
    next_layout: AtomicU32,
    boot_time: u64,
}

#[derive(Clone, Debug)]
pub struct PnfsBlockHandle {
    pub worker_id: u32,
    pub block: ExtendedBlock,
}

#[derive(Clone)]
pub struct PnfsDataServer {
    local_worker: WorkerAddress,
    fs_context: Arc<FsContext>,
    verifier: Arc<[u8]>,
}

impl PnfsManager {
    pub fn new() -> Self {
        let boot_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            layouts: RwLock::new(HashMap::new()),
            devices: RwLock::new(HashMap::new()),
            next_layout: AtomicU32::new(1),
            boot_time,
        }
    }

    pub fn deviceid(&self, fileid: Fileid4) -> [u8; 16] {
        encode_deviceid(fileid)
    }

    pub fn block_handles(&self, blocks: &FileBlocks, verifier: &[u8]) -> Vec<Nfs4FileHandle> {
        blocks
            .block_locs
            .iter()
            .map(|block| encode_block_fh(&block.block, block.locs.first(), verifier))
            .collect()
    }

    pub fn issue_layout(&self, clientid: u64, fileid: Fileid4, blocks: FileBlocks) -> Stateid4 {
        let deviceid = self.deviceid(fileid);
        let stateid = self.next_layout_stateid();

        self.layouts.write().unwrap().insert(
            stateid.other,
            LayoutState {
                stateid,
                clientid,
                fileid,
                deviceid,
            },
        );
        let mut devices = self.devices.write().unwrap();
        devices
            .entry(deviceid)
            .and_modify(|entry| {
                entry.blocks = blocks.clone();
                entry.refs += 1;
            })
            .or_insert(DeviceState { blocks, refs: 1 });

        stateid
    }

    pub fn return_layout(&self, stateid: &Stateid4) -> Option<LayoutState> {
        let layout = self.layouts.write().unwrap().remove(&stateid.other)?;
        let mut devices = self.devices.write().unwrap();
        if let Some(device) = devices.get_mut(&layout.deviceid) {
            if device.refs > 1 {
                device.refs -= 1;
            } else {
                devices.remove(&layout.deviceid);
            }
        }
        Some(layout)
    }

    pub fn device(&self, deviceid: &[u8; 16]) -> Option<FileBlocks> {
        self.devices
            .read()
            .unwrap()
            .get(deviceid)
            .map(|device| device.blocks.clone())
    }

    pub fn layout(&self, stateid: &Stateid4) -> Option<LayoutState> {
        self.layouts.read().unwrap().get(&stateid.other).cloned()
    }

    pub fn release_client(&self, clientid: u64) {
        let stateids: Vec<Stateid4> = self
            .layouts
            .read()
            .unwrap()
            .values()
            .filter(|layout| layout.clientid == clientid)
            .map(|layout| layout.stateid)
            .collect();

        for stateid in stateids {
            let _ = self.return_layout(&stateid);
        }
    }

    fn next_layout_stateid(&self) -> Stateid4 {
        let seq = self.next_layout.fetch_add(1, Ordering::Relaxed);
        let mut other = [0u8; 12];
        other[0..4].copy_from_slice(&0x504E4653u32.to_le_bytes());
        other[4..8].copy_from_slice(&(self.boot_time as u32).to_le_bytes());
        other[8..12].copy_from_slice(&seq.to_le_bytes());
        Stateid4::new(1, other)
    }
}

impl PnfsDataServer {
    pub fn new(local_worker: WorkerAddress, fs_context: Arc<FsContext>, verifier: Vec<u8>) -> Self {
        Self {
            local_worker,
            fs_context,
            verifier: Arc::from(verifier.into_boxed_slice()),
        }
    }

    pub fn resolve_handle(&self, fh: &Nfs4FileHandle) -> Nfs4Result<Option<PnfsBlockHandle>> {
        let Some(handle) = decode_verified_block_fh(fh, &self.verifier) else {
            return Ok(None);
        };

        if handle.worker_id != self.local_worker.worker_id {
            return Err(Nfs4Status::Stale.into());
        }

        Ok(Some(handle))
    }

    pub fn attrs(&self, handle: &PnfsBlockHandle) -> FileAttrs {
        let now = Nfstime4::now();
        let size = handle.block.len.max(0) as u64;
        FileAttrs {
            file_type: Nfs4FileType::Regular,
            mode: 0o444,
            nlink: 1,
            owner: "0".to_string(),
            group: "0".to_string(),
            size,
            used: size.div_ceil(512) * 512,
            fileid: ds_fileid(handle.worker_id, handle.block.id),
            atime: now,
            mtime: now,
            ctime: now,
        }
    }

    pub async fn read(
        &self,
        handle: &PnfsBlockHandle,
        stateid: &Stateid4,
        offset: u64,
        count: u32,
    ) -> Nfs4Result<(Vec<DataSlice>, bool)> {
        if !stateid.is_special() {
            return Err(Nfs4Status::BadStateid.into());
        }

        let block_len = handle.block.len.max(0) as u64;
        if offset >= block_len || count == 0 {
            return Ok((Vec::new(), true));
        }

        let read_len = (block_len - offset).min(count as u64) as i64;
        let block = handle.block.clone();
        let is_local = self.fs_context.is_local_worker(&self.local_worker);
        let mut slices = Vec::new();

        if is_local {
            let mut reader = BlockReaderLocal::new(
                self.fs_context.clone(),
                block,
                self.local_worker.clone(),
                offset as i64,
                read_len,
            )
            .await
            .map_err(Nfs4Error::from)?;

            while reader.remaining() > 0 {
                let slice = reader.read().await.map_err(Nfs4Error::from)?;
                if slice.len() == 0 {
                    break;
                }
                slices.push(slice);
            }
            reader.complete().await.map_err(Nfs4Error::from)?;
        } else {
            let mut reader = BlockReaderRemote::new(
                &self.fs_context,
                block,
                self.local_worker.clone(),
                offset as i64,
                read_len,
            )
            .await
            .map_err(Nfs4Error::from)?;

            while reader.remaining() > 0 {
                let slice = reader.read().await.map_err(Nfs4Error::from)?;
                if slice.len() == 0 {
                    break;
                }
                slices.push(slice);
            }
            reader.complete().await.map_err(Nfs4Error::from)?;
        }

        let bytes_read: u64 = slices.iter().map(|slice| slice.len() as u64).sum();
        Ok((slices, offset + bytes_read >= block_len))
    }
}

impl Default for PnfsManager {
    fn default() -> Self {
        Self::new()
    }
}

pub fn encode_layout_segment(
    offset: u64,
    length: u64,
    iomode: u32,
    deviceid: [u8; 16],
    stripe_size: u32,
    fhs: &[Nfs4FileHandle],
) -> std::io::Result<Vec<u8>> {
    let mut body = Vec::new();
    offset.serialize(&mut body)?;
    length.serialize(&mut body)?;
    iomode.serialize(&mut body)?;
    LAYOUT4_NFSV4_1_FILES.serialize(&mut body)?;
    encode_file_layout(deviceid, stripe_size, fhs)?.serialize(&mut body)?;
    Ok(body)
}

pub fn encode_device_addr(blocks: &FileBlocks, port: u16) -> std::io::Result<Vec<u8>> {
    let mut body = Vec::new();
    (blocks.block_locs.len() as u32).serialize(&mut body)?;
    for (idx, _) in blocks.block_locs.iter().enumerate() {
        (idx as u32).serialize(&mut body)?;
    }

    (blocks.block_locs.len() as u32).serialize(&mut body)?;
    for block in &blocks.block_locs {
        (block.locs.len() as u32).serialize(&mut body)?;
        for addr in &block.locs {
            b"tcp".to_vec().serialize(&mut body)?;
            encode_netaddr(addr, port).serialize(&mut body)?;
        }
    }

    Ok(body)
}

fn encode_file_layout(
    deviceid: [u8; 16],
    stripe_size: u32,
    fhs: &[Nfs4FileHandle],
) -> std::io::Result<Vec<u8>> {
    let mut body = Vec::new();
    body.write_all(&deviceid)?;
    (((stripe_size & NFL4_UFLG_STRIPE_UNIT_SIZE_MASK) | NFL4_UFLG_DENSE) as u32)
        .serialize(&mut body)?;
    0u32.serialize(&mut body)?; // first stripe index
    0u64.serialize(&mut body)?; // pattern offset
    (fhs.len() as u32).serialize(&mut body)?;
    for fh in fhs {
        fh.serialize(&mut body)?;
    }
    Ok(body)
}

pub fn encode_block_fh(
    block: &ExtendedBlock,
    first: Option<&WorkerAddress>,
    verifier: &[u8],
) -> Nfs4FileHandle {
    let mut data = Vec::with_capacity(28 + PNFS_BLOCK_FH_TAG_LEN);
    data.extend_from_slice(&PNFS_BLOCK_FH_MAGIC.to_le_bytes());
    data.extend_from_slice(&first.map(|w| w.worker_id).unwrap_or_default().to_le_bytes());
    data.extend_from_slice(&block.id.to_le_bytes());
    data.extend_from_slice(&block.len.to_le_bytes());
    data.extend_from_slice(&(block.storage_type as u32).to_le_bytes());
    let tag = block_fh_tag(&data, verifier);
    data.extend_from_slice(&tag[..PNFS_BLOCK_FH_TAG_LEN]);
    Nfs4FileHandle::new(data)
}

pub fn is_block_fh(fh: &Nfs4FileHandle) -> bool {
    fh.data.len() == 28 + PNFS_BLOCK_FH_TAG_LEN
        && u32::from_le_bytes(fh.data[0..4].try_into().unwrap_or_default()) == PNFS_BLOCK_FH_MAGIC
}

pub fn decode_block_fh(fh: &Nfs4FileHandle) -> Option<PnfsBlockHandle> {
    if !is_block_fh(fh) {
        return None;
    }

    let worker_id = u32::from_le_bytes(fh.data[4..8].try_into().ok()?);
    let block_id = i64::from_le_bytes(fh.data[8..16].try_into().ok()?);
    let block_len = i64::from_le_bytes(fh.data[16..24].try_into().ok()?);
    let storage_type = StorageType::try_from(i32::from_le_bytes(fh.data[24..28].try_into().ok()?))
        .ok()
        .unwrap_or(StorageType::Disk);

    Some(PnfsBlockHandle {
        worker_id,
        block: ExtendedBlock::new(block_id, block_len, storage_type, FileType::File),
    })
}

pub fn decode_verified_block_fh(fh: &Nfs4FileHandle, verifier: &[u8]) -> Option<PnfsBlockHandle> {
    if !is_block_fh(fh) {
        return None;
    }

    let body_len = 28;
    let expected = block_fh_tag(&fh.data[..body_len], verifier);
    if expected[..PNFS_BLOCK_FH_TAG_LEN] != fh.data[body_len..] {
        return None;
    }

    decode_block_fh(fh)
}

fn encode_deviceid(fileid: Fileid4) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0..4].copy_from_slice(&0x50504653u32.to_le_bytes());
    id[8..16].copy_from_slice(&fileid.to_le_bytes());
    id
}

fn encode_netaddr(addr: &WorkerAddress, port: u16) -> Vec<u8> {
    let p_hi = (port >> 8) as u8;
    let p_lo = (port & 0xFF) as u8;
    let host = Ipv4Addr::from_str(&addr.ip_addr)
        .map(|ip| {
            ip.octets()
                .iter()
                .map(u8::to_string)
                .collect::<Vec<_>>()
                .join(".")
        })
        .unwrap_or_else(|_| addr.ip_addr.clone());
    format!("{}.{}.{}", host, p_hi, p_lo).into_bytes()
}

fn ds_fileid(_worker_id: u32, block_id: i64) -> u64 {
    block_id as u64
}

fn block_fh_tag(body: &[u8], verifier: &[u8]) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;

    let mut mac =
        HmacSha256::new_from_slice(verifier).expect("pnfs ds verifier must be a valid HMAC key");
    mac.update(body);
    mac.finalize().into_bytes().into()
}
