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

use curvine_common::utils::SerdeUtils;
use orpc::common::{FileUtils, LocalTime, Utils};
use orpc::{err_box, try_err, CommonResult};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

const SNAPSHOT_MANIFEST_FILE: &str = "_curvine_snapshot_manifest";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub marker_op_id: u64,
    pub node_id: u64,
    // Lightweight file-set fingerprint used to bind a snapshot marker to the
    // checkpoint directory. It is not a content checksum; snapshot byte
    // integrity is handled by the Raft snapshot transfer/storage layer.
    #[serde(alias = "checkpoint_checksum")]
    pub checkpoint_fingerprint: u128,
    pub created_ms: u64,
}

impl SnapshotManifest {
    pub fn write_checkpoint(dir: &str, marker_op_id: u64, node_id: u64) -> CommonResult<Self> {
        let checkpoint_fingerprint = checkpoint_fingerprint(dir)?;
        let manifest = Self {
            marker_op_id,
            node_id,
            checkpoint_fingerprint,
            created_ms: LocalTime::mills(),
        };
        let manifest_path = manifest_path(dir);
        FileUtils::create_parent_dir(&manifest_path, true)?;
        let bytes = SerdeUtils::serialize(&manifest)?;
        fs::write(manifest_path, bytes)?;
        Ok(manifest)
    }

    pub fn read_checkpoint(dir: &str) -> CommonResult<Self> {
        let manifest_path = manifest_path(dir);
        let bytes = try_err!(fs::read(&manifest_path));
        let manifest: Self = SerdeUtils::deserialize(&bytes)?;
        let fingerprint = checkpoint_fingerprint(dir)?;
        if fingerprint != manifest.checkpoint_fingerprint {
            return err_box!(
                "checkpoint fingerprint mismatch for {}: manifest={}, actual={}",
                dir,
                manifest.checkpoint_fingerprint,
                fingerprint
            );
        }
        Ok(manifest)
    }

    pub fn validate_checkpoint(
        dir: &str,
        expected_op_id: u64,
        expected_node_id: u64,
    ) -> CommonResult<()> {
        let manifest = Self::read_checkpoint(dir)?;
        if manifest.marker_op_id != expected_op_id {
            return err_box!(
                "snapshot marker op_id mismatch for {}: manifest={}, expected={}",
                dir,
                manifest.marker_op_id,
                expected_op_id
            );
        }
        if manifest.node_id != expected_node_id {
            return err_box!(
                "snapshot marker node_id mismatch for {}: manifest={}, expected={}",
                dir,
                manifest.node_id,
                expected_node_id
            );
        }
        Ok(())
    }

    pub fn validate_checkpoint_if_present(
        dir: &str,
        expected_op_id: u64,
        expected_node_id: u64,
    ) -> CommonResult<()> {
        let manifest_path = manifest_path(dir);
        match fs::metadata(&manifest_path) {
            Ok(_) => Self::validate_checkpoint(dir, expected_op_id, expected_node_id),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn manifest_path(dir: &str) -> PathBuf {
    Path::new(dir).join(SNAPSHOT_MANIFEST_FILE)
}

fn checkpoint_fingerprint(dir: &str) -> CommonResult<u128> {
    let mut files = FileUtils::list_files(dir, false)?;
    files.retain(|path| path != SNAPSHOT_MANIFEST_FILE);
    files.sort_unstable();

    let mut checksum = 0u128;
    for relative in files {
        let path = Path::new(dir).join(&relative);
        checksum = checksum.wrapping_add(Utils::crc32(relative.as_bytes()) as u128);
        checksum = checksum.wrapping_add(fs::metadata(path)?.len() as u128);
    }
    Ok(checksum)
}
