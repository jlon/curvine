# Code Review: Scope B Filesystem Semantics

Goal: read-only review of `/mnt/data/curvine-fine-locks` branch `feat/master-metadata-fine-grained-locks`, scoped to filesystem semantics and current metadata/cache paths relative to `main`.

Skill perspective check:
- `cv-submit-pr-review` was loaded from the project skill.
- `remove-ai-slops` and `programming` SKILL.md files were not available in the searched skill paths (`/home/oppo/.codex/skills`, `/home/oppo/.agents/skills`, and project `.agents/skills`), so their documented criteria from the prompt were applied manually.
- No deletion-only, tautological, or implementation-constant-only tests were identified in the inspected scope. The diff does violate the programming perspective through correctness-risky optimistic cache/lock paths noted below.

Evidence inspected:
- Current branch: `feat/master-metadata-fine-grained-locks`
- Base: local `main`
- Diff scope: tracked `git diff main`
- Read-only verification: `git diff --check main -- curvine-server/src/master/fs/master_filesystem.rs curvine-server/src/master/meta/fs_dir.rs curvine-server/src/master/meta/store/inode_store.rs curvine-server/src/master/meta/metadata_replica_reader.rs curvine-server/tests/master_fs_test.rs` passed.
- Tests were not run in this review pass.

## CRITICAL

No findings.

## HIGH

1. Forced symlink rewrite deletes the backing inode even when other hard-link aliases still reference it.

References:
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/meta/fs_dir.rs:1327`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/meta/fs_dir.rs:1343`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/meta/store/inode_store.rs:462`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/meta/store/inode_store.rs:475`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/meta/store/inode_store.rs:478`
- `/mnt/data/curvine-fine-locks/curvine-server/tests/master_fs_test.rs:1469`

`unprotected_symlink` resolves an existing `FileEntry` to the full stored inode before a forced symlink rewrite, then passes that full inode as `replaced_inode` into `apply_symlink`. `apply_symlink` unconditionally calls `batch.delete_inode(inode.id())` for the replaced inode and writes the new inode/edge for only the overwritten path. The existing tests prove Curvine allows hard-linking symlink inodes (`fs.link("/a/symbolic", "/a/nick")` and both paths report `nlink == 2`), so a forced rewrite of `/a/symbolic` must only remove that directory edge and decrement the old inode's nlink. The current code deletes the old inode while `/a/nick` still points to the same inode id, leaving the alias resolving through `FileEntry` to a missing inode or returning stale cached status if that alias was cached. This directly breaks hard link + symlink semantics and the current metadata cache path.

2. Same-parent rename fast path can apply a stale parent handle after a concurrent parent-directory move, then journal a path that replay will ignore.

References:
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/fs/master_filesystem.rs:1649`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/fs/master_filesystem.rs:1657`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/fs/master_filesystem.rs:1756`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/fs/master_filesystem.rs:1766`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/meta/fs_dir.rs:443`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/meta/fs_dir.rs:556`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/journal/journal_loader.rs:867`
- `/mnt/data/curvine-fine-locks/curvine-server/src/master/journal/journal_loader.rs:869`

`lock_rename_paths` returns immediately when `candidate.same_parent` is present, without the later `stable_metadata_rename_lock_plan` revalidation used by the non-fast path. The same-parent lock plan deliberately leaves the parent as read-locked (`parent_write = same_parent.is_none()`), so another topology write can move the parent directory after the candidate resolve but before `rename_same_parent` mutates the saved `DirectoryChildren` handle. `rename_same_parent` only validates child entries inside that handle; it does not revalidate that the requested source and destination strings still reach this parent. If the parent move journals first, the leader can still mutate the moved directory through the stale handle and then log `src`/`dst` strings that no longer resolve. Follower replay resolves the source path and returns early when missing, causing live/replay divergence.

## MEDIUM

No findings.

## LOW

No findings.

## Still Needs Verification

- Add/execute a hard-link symlink boundary test: create symlink, hard-link it, force-rewrite one path, then assert the alias still resolves and nlink/target semantics remain correct after live reads and restore/replay.
- Add/execute a concurrent parent-move plus same-parent child-rename replay test to verify leader/follower hashes stay equal.
- Run the relevant Curvine filesystem test target after fixes.

## Status

codeQualityStatus: BLOCK
recommendation: REQUEST_CHANGES
blockers:
- Fix forced symlink rewrite so replacing one directory entry does not delete a hard-linked symlink inode still referenced by other entries, and invalidate affected cache entries.
- Revalidate same-parent rename paths after locks, or acquire a lock that prevents parent topology movement before using the saved `SameParentRenamePlan`.
