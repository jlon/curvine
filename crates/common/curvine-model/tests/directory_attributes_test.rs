// Copyright 2026 OPPO.
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

use curvine_model::state::{DirectoryAttributeDelta, DirectoryAttributes};

fn legacy_delta(mtime: i64, ctime: i64, nlink_delta: i32) -> [u8; 24] {
    let mut bytes = [0; 24];
    bytes[..4].copy_from_slice(b"CDD1");
    bytes[4..12].copy_from_slice(&mtime.to_le_bytes());
    bytes[12..20].copy_from_slice(&ctime.to_le_bytes());
    bytes[20..24].copy_from_slice(&nlink_delta.to_le_bytes());
    bytes
}

#[test]
fn directory_attribute_delta_is_commutative() {
    let first = DirectoryAttributeDelta::new(10, 12, 1);
    let second = DirectoryAttributeDelta::new(15, 14, -1);

    assert_eq!(first.combine(second), second.combine(first));

    let mut attributes = DirectoryAttributes::new(1, 2, 3);
    attributes.apply(first.combine(second).unwrap()).unwrap();
    assert_eq!(attributes, DirectoryAttributes::new(15, 14, 3));
}

#[test]
fn directory_attribute_encoding_rejects_other_record_types() {
    let attributes = DirectoryAttributes::new(11, 12, 13);
    let delta = DirectoryAttributeDelta::new(14, 15, 1);

    assert_eq!(
        DirectoryAttributes::decode(&attributes.encode()),
        Some(attributes)
    );
    assert_eq!(
        DirectoryAttributeDelta::decode(&delta.encode()),
        Some(delta)
    );
    assert_eq!(DirectoryAttributes::decode(&delta.encode()), None);
    assert_eq!(DirectoryAttributeDelta::decode(&attributes.encode()), None);
}

#[test]
fn directory_attribute_delta_preserves_legacy_base_order() {
    let legacy = DirectoryAttributeDelta::decode(&legacy_delta(15, 15, 1)).unwrap();
    let base = DirectoryAttributes::new(10, 10, 2);
    let current = DirectoryAttributeDelta::new(20, 20, 1).with_base(base);

    assert_eq!(
        current.combine(legacy),
        Some(DirectoryAttributeDelta::new(20, 20, 2).with_base(base))
    );
    assert_eq!(legacy.combine(current), None);
    assert_eq!(
        current.combine(
            DirectoryAttributeDelta::new(25, 25, 1).with_base(DirectoryAttributes::new(11, 11, 2)),
        ),
        None
    );
}
