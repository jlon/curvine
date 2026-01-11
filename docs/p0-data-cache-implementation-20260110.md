# P0 Optimization: Small File Data Cache Implementation

**Date:** 2026-01-10 18:30:00 (UTC+8)
**Optimization Type:** Read Performance (4-5x expected improvement)
**Target:** Small files (<= 64KB)

---

## 🎯 Optimization Goal

Reduce backend I/O for frequently accessed small files by implementing an in-memory data cache.

**Problem**: Every READ operation queries backend (15ms latency)
**Solution**: Cache small file contents in memory (< 1ms access)
**Expected**: Read throughput 58.68 → 230+ files/sec (**4x improvement**)

---

## 📊 Implementation Summary

### Modified Files

1. **curvine-common/src/conf/cluster_conf.rs**
   - Added 3 new configuration parameters
   - Default: 1000 files, 10s TTL, 64KB max size

2. **curvine-nfs/src/nfs4/fs.rs**
   - Added `data_cache` field to Nfs4FileSystem
   - Implemented 3 cache methods: get/put/invalidate

3. **curvine-nfs/src/nfs4/ops/read.rs**
   - Added cache lookup before backend read
   - Added cache storage after successful read
   - Only caches complete file reads (offset=0, eof=true)

4. **curvine-nfs/src/nfs4/ops/write.rs**
   - Added cache invalidation after WRITE

5. **curvine-nfs/src/nfs4/ops/setattr.rs**
   - Added cache invalidation after SETATTR (size change)

---

## 🔧 Technical Details

### Cache Strategy

```rust
// Cache eligibility criteria:
1. offset == 0 (reading from beginning)
2. eof == true (complete file read)
3. file_size <= max_cacheable_file_size (64KB default)
4. data_cache enabled (file_data_cache_size > 0)
```

### Cache Operations

**Read Path**:
```
READ request (offset=0)
  ↓
Check cache
  ├→ Cache HIT → Return immediately (< 1ms)
  └→ Cache MISS → Read from backend (15ms)
                 → Store in cache if eligible
                 → Return data
```

**Write Path**:
```
WRITE request
  ↓
Write to backend
  ↓
Invalidate cache (file content changed)
```

**SETATTR Path**:
```
SETATTR request (size modification)
  ↓
Modify attributes
  ↓
Invalidate cache (file size changed)
```

### Cache Configuration

Default values in `NfsGatewayConf`:
```rust
file_data_cache_size: 1000,         // Max 1000 files
file_data_cache_ttl_secs: 10,       // 10 seconds TTL
max_cacheable_file_size: 65536,     // 64KB max
```

Memory usage:
- Max: 1000 files × 64KB = 64MB
- Typical: ~20-30MB (smaller files)

---

## 📈 Performance Analysis

### Expected Cache Hit Rate

Small file workload (1KB files, repeated reads):
- First read: Cache MISS (15ms)
- Subsequent reads within 10s: Cache HIT (< 1ms)
- **Expected hit rate: 70-90%**

### Performance Calculation

```
Current performance:
- Average latency: 17ms (2ms network + 15ms backend)
- Throughput: 58.68 files/sec

With data cache (assuming 80% hit rate):
- Average latency: 0.8 × 1ms + 0.2 × 17ms = 4.2ms
- Throughput: 1000ms / 4.2ms = 238 files/sec
- Improvement: 238 / 58.68 = 4.05x ✅
```

---

## ✅ Safety Features

### Cache Consistency

1. **Write Invalidation**:
   - Any WRITE immediately invalidates cache
   - Ensures readers see updated content

2. **Size Change Invalidation**:
   - SETATTR (size) immediately invalidates cache
   - Handles truncate/extend operations

3. **TTL Expiration**:
   - Cache entries auto-expire after 10 seconds
   - Prevents stale data accumulation

4. **LRU Eviction**:
   - Moka cache uses LRU eviction
   - Oldest entries evicted when capacity reached

### Memory Safety

- Fixed capacity (1000 files)
- Size limit per file (64KB)
- Maximum memory usage bounded (64MB)
- No memory leaks (Arc reference counting)

---

## 🧪 Testing Plan

### Test Cases

1. **Cache Hit Test**:
   ```bash
   # Read same file twice
   cat /mnt/nfs/test.txt
   cat /mnt/nfs/test.txt  # Should be much faster
   ```

2. **Cache Invalidation Test**:
   ```bash
   cat /mnt/nfs/test.txt   # Cache MISS + STORE
   echo "new" > /mnt/nfs/test.txt  # Invalidate
   cat /mnt/nfs/test.txt   # Cache MISS again (correct!)
   ```

3. **Performance Test**:
   ```bash
   scripts/nfs_perf_test.sh
   # Compare with P1 baseline
   # Expected: Read 58.68 → 230+ files/sec
   ```

### Success Criteria

- ✅ Read throughput > 200 files/sec (3.4x improvement minimum)
- ✅ No data corruption (cache invalidation works)
- ✅ Memory usage < 100MB
- ✅ No regression on write performance

---

## 🔍 Logging and Debugging

### Cache Hit/Miss Logging

```rust
// Cache HIT (debug level)
tracing::debug!(
    "Data cache HIT: fileid={}, cached_size={}, ...",
    fileid, data_len
);

// Cache MISS+STORE (debug level)
tracing::debug!(
    "Data cache MISS+STORE: fileid={}, size={} bytes",
    fileid, total_size
);

// Cache Invalidation (debug level)
tracing::debug!("Data cache: invalidated fileid={}", fileid);
```

Enable debug logging:
```bash
RUST_LOG=curvine_nfs=debug ./curvine-nfs-gateway
```

---

## 📝 Comparison with Fuse Implementation

### Similarities

Both NFS and Fuse use Moka cache with TTL.

### Differences

| Aspect | Fuse | NFS (This Implementation) |
|--------|------|---------------------------|
| Cache Type | Metadata only | **Data + Metadata** |
| Cache Trigger | READDIR | **READ (offset=0)** |
| Invalidation | - | **WRITE + SETATTR** |
| Use Case | Stat acceleration | **Read acceleration** |

**Key Innovation**: NFS data cache is unique - Fuse doesn't have this!

---

## 🚀 Next Steps

### Immediate (After Build)

1. Deploy compiled binary to build/dist/lib/
2. Restart curvine cluster (底层)
3. Restart curvine-nfs-gateway
4. Run performance test

### Post-Testing

If test succeeds (Read > 200 files/sec):
- Document results in performance report
- Consider P1 optimization (Attribute pre-warming)

If test fails:
- Check debug logs for cache hit rate
- Verify cache invalidation works correctly
- Analyze backend latency

---

## 📊 Monitoring Metrics

### Key Metrics to Track

1. **Cache Hit Rate**: Target 70-90%
2. **Read Throughput**: Target > 200 files/sec
3. **Read Latency**: Target < 5ms average
4. **Memory Usage**: Should stay < 64MB

### Debug Commands

```bash
# Check cache stats (if available)
grep "Data cache" logs/nfs-gateway.log

# Monitor memory usage
ps aux | grep curvine-nfs-gateway

# Test cache hit
time cat /mnt/nfs/test.txt  # First read (miss)
time cat /mnt/nfs/test.txt  # Second read (hit)
```

---

## 🎓 Lessons Learned

### Why This Works

1. **Small Files**: 1KB files fit entirely in memory
2. **Repeated Reads**: Test workload reads same files multiple times
3. **Low Memory Cost**: 1000 × 64KB = 64MB (acceptable)
4. **Simple Invalidation**: Write/SetAttr are easy to detect

### Why Not Larger Files?

- 1MB file cache → 1000 files = 1GB memory (too much!)
- Large files benefit more from read-ahead (already implemented)
- Large files rarely read multiple times

### Why 10s TTL?

- Short enough to avoid stale data
- Long enough for repeated reads in tests
- Balances consistency and performance

---

**Implementation Date:** 2026-01-10
**Status:** ✅ Code Complete, Build In Progress
**Next:** Deploy and Test

