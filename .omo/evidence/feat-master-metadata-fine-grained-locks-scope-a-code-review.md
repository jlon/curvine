# Code Review: Scope A Metadata Replica Reader

Goal: read-only review of `/mnt/data/curvine-fine-locks` current final worktree, scoped to `metadata_replica_reader.rs` PathCache/FIFO/weight, FileInodeCache, epoch restore, and cache invalidation. This review did not expand into unrelated history diff.

Skill perspective check:
- Project `cv-submit-pr-review` skill was loaded from `/mnt/data/curvine-fine-locks/.agents/skills/cv-submit-pr-review/SKILL.md`.
- `remove-ai-slops` and `programming` SKILL.md files were not available in the searched skill paths (`/home/oppo/.codex/skills`, `/home/oppo/.agents/skills`, `/mnt/data/curvine/.agents/skills`, `/mnt/data/curvine-fine-locks/.agents/skills`), so the documented criteria from the prompt were applied manually.
- No deletion-only, tautological, implementation-constant-only, or brittle prompt-style tests were identified in this scope.
- The inspected diff does not violate the remove-ai-slops or programming perspectives in this scope: the cache weighting, epoch checks, restore invalidation, and FileInodeCache validation are tied to the requested metadata read correctness/performance boundary rather than unrelated extraction or normalization.

Evidence inspected:
- Worktree: `/mnt/data/curvine-fine-locks`
- Branch: `feat/master-metadata-fine-grained-locks`
- HEAD: `c6c70a88`
- Current file inspected in full: `/mnt/data/curvine-fine-locks/curvine-server/src/master/meta/metadata_replica_reader.rs`
- Direct callers/restore/invalidation inspected:
  - `/mnt/data/curvine-fine-locks/curvine-server/src/master/meta/fs_dir.rs`
  - `/mnt/data/curvine-fine-locks/curvine-server/src/master/fs/master_filesystem.rs`
  - `/mnt/data/curvine-fine-locks/curvine-server/src/master/meta/inode/inodes_children.rs`
  - `/mnt/data/curvine-fine-locks/curvine-common/src/conf/master_conf.rs`
- Relevant tests inspected:
  - `/mnt/data/curvine-fine-locks/curvine-server/tests/master_fs_test.rs`
- Read-only verification:
  - `git diff --check -- curvine-server/src/master/meta/metadata_replica_reader.rs` passed.
- Tests were not executed because this review was requested as read-only/no file modification; running Rust tests would write build/test artifacts.

## CRITICAL

No findings.

## HIGH

No findings.

## MEDIUM

No findings.

## LOW

No findings.

## Notes

- `replace_root` publishes an odd/even epoch around root replacement and invalidates FileInodeCache; stale thread-local PathCache entries are rejected by epoch/version validation before returning results.
- FileInodeCache lookups and insertions validate both the file-status version shard and stable root epoch before returning or admitting entries.
- PathCache FIFO stale records are bounded and compacted, and entries over the configured weight cap are immediately evicted.
- Mutation paths inspected for file inode cache invalidation include rename/delete/free/set_attr/add_block/complete_file/block report/delete worker locations.

## Status

codeQualityStatus: CLEAR
recommendation: APPROVE
blockers: []
