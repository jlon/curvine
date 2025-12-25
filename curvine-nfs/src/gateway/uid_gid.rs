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

//! UID/GID mapping utilities
//!
//! Converts between Curvine's string-based owner/group and NFS's numeric UID/GID.
//! Reuses orpc::sys for system lookups (same as curvine-fuse).

use orpc::sys;

/// Resolve owner string to UID
///
/// Priority: numeric string > system lookup > default
///
/// # Arguments
/// * `owner` - Owner string (can be numeric "1000" or username "root")
/// * `default_uid` - Default UID if resolution fails
#[inline]
pub fn resolve_uid(owner: &str, default_uid: u32) -> u32 {
    if owner.is_empty() {
        return default_uid;
    }

    // Fast path: try parse as numeric first
    if let Ok(uid) = owner.parse::<u32>() {
        return uid;
    }

    // Slow path: lookup by username
    sys::get_uid_by_name(owner).unwrap_or(default_uid)
}

/// Resolve group string to GID
///
/// Priority: numeric string > system lookup > default
///
/// # Arguments
/// * `group` - Group string (can be numeric "1000" or group name "wheel")
/// * `default_gid` - Default GID if resolution fails
#[inline]
pub fn resolve_gid(group: &str, default_gid: u32) -> u32 {
    if group.is_empty() {
        return default_gid;
    }

    // Fast path: try parse as numeric first
    if let Ok(gid) = group.parse::<u32>() {
        return gid;
    }

    // Slow path: lookup by group name
    sys::get_gid_by_name(group).unwrap_or(default_gid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_uid_numeric() {
        assert_eq!(resolve_uid("1000", 65534), 1000);
        assert_eq!(resolve_uid("0", 65534), 0);
    }

    #[test]
    fn test_resolve_uid_empty() {
        assert_eq!(resolve_uid("", 65534), 65534);
    }

    #[test]
    fn test_resolve_gid_numeric() {
        assert_eq!(resolve_gid("1000", 65534), 1000);
        assert_eq!(resolve_gid("0", 65534), 0);
    }

    #[test]
    fn test_resolve_gid_empty() {
        assert_eq!(resolve_gid("", 65534), 65534);
    }
}
