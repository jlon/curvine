#!/bin/bash
# Comprehensive benchmark: NFS Gateway vs FUSE
# Runs multiple iterations and calculates average performance

set -e

NFS_MOUNT="/mnt/curvine-nfs"
FUSE_MOUNT="/curvine-fuse"
ITERATIONS=3

echo "=========================================="
echo "Curvine NFS Gateway vs FUSE Benchmark"
echo "=========================================="
echo "NFS Mount: ${NFS_MOUNT}"
echo "FUSE Mount: ${FUSE_MOUNT}"
echo "Iterations: ${ITERATIONS}"
echo ""

# Check if mount points exist
if [ ! -d "${NFS_MOUNT}" ]; then
    echo "Error: NFS mount point ${NFS_MOUNT} does not exist"
    exit 1
fi

if [ ! -d "${FUSE_MOUNT}" ]; then
    echo "Error: FUSE mount point ${FUSE_MOUNT} does not exist"
    exit 1
fi

# Function to run fio test
run_fio_test() {
    local name=$1
    local mount=$2
    local testfile=$3
    local bs=$4
    local rw=$5
    local direct=$6
    local ioengine=$7
    local iodepth=$8
    local numjobs=$9
    local size=${10:-""}
    
    if [ -n "${size}" ]; then
        fio --name="${name}" \
            --filename="${testfile}" \
            --size="${size}" \
            --bs="${bs}" \
            --rw="${rw}" \
            --direct="${direct}" \
            --ioengine="${ioengine}" \
            --iodepth="${iodepth}" \
            --numjobs="${numjobs}" \
            --runtime=30 \
            --time_based \
            --group_reporting \
            --output-format=json \
            --output="${name}.json"
    else
        fio --name="${name}" \
            --filename="${testfile}" \
            --bs="${bs}" \
            --rw="${rw}" \
            --direct="${direct}" \
            --ioengine="${ioengine}" \
            --iodepth="${iodepth}" \
            --numjobs="${numjobs}" \
            --runtime=30 \
            --time_based \
            --group_reporting \
            --output-format=json \
            --output="${name}.json"
    fi
}

# Function to extract bandwidth from fio json output
extract_bw() {
    local jsonfile=$1
    local rw_type=$2  # "read" or "write"
    
    if [ "${rw_type}" = "read" ]; then
        python3 -c "import json; data=json.load(open('${jsonfile}')); print(int(data['jobs'][0]['read']['bw_bytes']/1024/1024))"
    else
        python3 -c "import json; data=json.load(open('${jsonfile}')); print(int(data['jobs'][0]['write']['bw_bytes']/1024/1024))"
    fi
}

# Create test files if needed
echo "Preparing test files..."
NFS_SEQ_FILE="${NFS_MOUNT}/fio_seq_test"
FUSE_SEQ_FILE="${FUSE_MOUNT}/fio_seq_test"
NFS_RAND_FILE="${NFS_MOUNT}/fio_rand_test"
FUSE_RAND_FILE="${FUSE_MOUNT}/fio_rand_test"

# Create 1GB files for sequential tests
if [ ! -f "${NFS_SEQ_FILE}" ] || [ $(stat -c%s "${NFS_SEQ_FILE}") -lt 1073741824 ]; then
    echo "Creating NFS sequential test file (1GB)..."
    dd if=/dev/zero of="${NFS_SEQ_FILE}" bs=1M count=1024 status=progress
fi

if [ ! -f "${FUSE_SEQ_FILE}" ] || [ $(stat -c%s "${FUSE_SEQ_FILE}") -lt 1073741824 ]; then
    echo "Creating FUSE sequential test file (1GB)..."
    dd if=/dev/zero of="${FUSE_SEQ_FILE}" bs=1M count=1024 status=progress
fi

# Create 1GB files for random tests
if [ ! -f "${NFS_RAND_FILE}" ] || [ $(stat -c%s "${NFS_RAND_FILE}") -lt 1073741824 ]; then
    echo "Creating NFS random test file (1GB)..."
    dd if=/dev/zero of="${NFS_RAND_FILE}" bs=1M count=1024 status=progress
fi

if [ ! -f "${FUSE_RAND_FILE}" ] || [ $(stat -c%s "${FUSE_RAND_FILE}") -lt 1073741824 ]; then
    echo "Creating FUSE random test file (1GB)..."
    dd if=/dev/zero of="${FUSE_RAND_FILE}" bs=1M count=1024 status=progress
fi

echo ""
echo "=========================================="
echo "Test 1: Sequential Read (1M blocks, direct I/O)"
echo "=========================================="

for i in $(seq 1 ${ITERATIONS}); do
    echo "Iteration $i/${ITERATIONS}..."
    
    # Clear cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    
    # NFS test
    run_fio_test "nfs_seq_read_${i}" "${NFS_MOUNT}" "${NFS_SEQ_FILE}" "1M" "read" 1 "psync" 1 1
    
    # Clear cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    
    # FUSE test
    run_fio_test "fuse_seq_read_${i}" "${FUSE_MOUNT}" "${FUSE_SEQ_FILE}" "1M" "read" 1 "psync" 1 1
done

echo ""
echo "=========================================="
echo "Test 2: Sequential Write (1M blocks, direct I/O)"
echo "=========================================="

for i in $(seq 1 ${ITERATIONS}); do
    echo "Iteration $i/${ITERATIONS}..."
    
    # Clear cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    
    # NFS test
    run_fio_test "nfs_seq_write_${i}" "${NFS_MOUNT}" "${NFS_MOUNT}/fio_write_test_${i}" "1M" "write" 1 "psync" 1 1 "100M"
    
    # Clear cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    
    # FUSE test
    run_fio_test "fuse_seq_write_${i}" "${FUSE_MOUNT}" "${FUSE_MOUNT}/fio_write_test_${i}" "1M" "write" 1 "psync" 1 1 "100M"
done

echo ""
echo "=========================================="
echo "Test 3: Random Read 4K (direct I/O)"
echo "=========================================="

for i in $(seq 1 ${ITERATIONS}); do
    echo "Iteration $i/${ITERATIONS}..."
    
    # Clear cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    
    # NFS test
    run_fio_test "nfs_rand_read_4k_${i}" "${NFS_MOUNT}" "${NFS_RAND_FILE}" "4K" "randread" 1 "psync" 1 1
    
    # Clear cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    
    # FUSE test
    run_fio_test "fuse_rand_read_4k_${i}" "${FUSE_MOUNT}" "${FUSE_RAND_FILE}" "4K" "randread" 1 "psync" 1 1
done

echo ""
echo "=========================================="
echo "Test 4: Random Read 64K (direct I/O)"
echo "=========================================="

for i in $(seq 1 ${ITERATIONS}); do
    echo "Iteration $i/${ITERATIONS}..."
    
    # Clear cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    
    # NFS test
    run_fio_test "nfs_rand_read_64k_${i}" "${NFS_MOUNT}" "${NFS_RAND_FILE}" "64K" "randread" 1 "psync" 1 1
    
    # Clear cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    
    # FUSE test
    run_fio_test "fuse_rand_read_64k_${i}" "${FUSE_MOUNT}" "${FUSE_RAND_FILE}" "64K" "randread" 1 "psync" 1 1
done

echo ""
echo "=========================================="
echo "Calculating Results..."
echo "=========================================="

# Calculate averages
calc_avg() {
    local sum=0
    local count=0
    for val in "$@"; do
        sum=$((sum + val))
        count=$((count + 1))
    done
    echo $((sum / count))
}

# Sequential Read
nfs_seq_read_bws=()
fuse_seq_read_bws=()
for i in $(seq 1 ${ITERATIONS}); do
    nfs_seq_read_bws+=($(extract_bw "nfs_seq_read_${i}.json" "read"))
    fuse_seq_read_bws+=($(extract_bw "fuse_seq_read_${i}.json" "read"))
done
nfs_seq_read_avg=$(calc_avg "${nfs_seq_read_bws[@]}")
fuse_seq_read_avg=$(calc_avg "${fuse_seq_read_bws[@]}")

# Sequential Write
nfs_seq_write_bws=()
fuse_seq_write_bws=()
for i in $(seq 1 ${ITERATIONS}); do
    nfs_seq_write_bws+=($(extract_bw "nfs_seq_write_${i}.json" "write"))
    fuse_seq_write_bws+=($(extract_bw "fuse_seq_write_${i}.json" "write"))
done
nfs_seq_write_avg=$(calc_avg "${nfs_seq_write_bws[@]}")
fuse_seq_write_avg=$(calc_avg "${fuse_seq_write_bws[@]}")

# Random Read 4K
nfs_rand_4k_bws=()
fuse_rand_4k_bws=()
for i in $(seq 1 ${ITERATIONS}); do
    nfs_rand_4k_bws+=($(extract_bw "nfs_rand_read_4k_${i}.json" "read"))
    fuse_rand_4k_bws+=($(extract_bw "fuse_rand_read_4k_${i}.json" "read"))
done
nfs_rand_4k_avg=$(calc_avg "${nfs_rand_4k_bws[@]}")
fuse_rand_4k_avg=$(calc_avg "${fuse_rand_4k_bws[@]}")

# Random Read 64K
nfs_rand_64k_bws=()
fuse_rand_64k_bws=()
for i in $(seq 1 ${ITERATIONS}); do
    nfs_rand_64k_bws+=($(extract_bw "nfs_rand_read_64k_${i}.json" "read"))
    fuse_rand_64k_bws+=($(extract_bw "fuse_rand_read_64k_${i}.json" "read"))
done
nfs_rand_64k_avg=$(calc_avg "${nfs_rand_64k_bws[@]}")
fuse_rand_64k_avg=$(calc_avg "${fuse_rand_64k_bws[@]}")

echo ""
echo "=========================================="
echo "Final Results (Average of ${ITERATIONS} runs)"
echo "=========================================="
echo ""
echo "Sequential Read (1M blocks):"
echo "  NFS:  ${nfs_seq_read_avg} MiB/s"
echo "  FUSE: ${fuse_seq_read_avg} MiB/s"
echo ""
echo "Sequential Write (1M blocks):"
echo "  NFS:  ${nfs_seq_write_avg} MiB/s"
echo "  FUSE: ${fuse_seq_write_avg} MiB/s"
echo ""
echo "Random Read 4K:"
echo "  NFS:  ${nfs_rand_4k_avg} MiB/s"
echo "  FUSE: ${fuse_rand_4k_avg} MiB/s"
echo ""
echo "Random Read 64K:"
echo "  NFS:  ${nfs_rand_64k_avg} MiB/s"
echo "  FUSE: ${fuse_rand_64k_avg} MiB/s"
echo ""

# Cleanup json files
rm -f *.json

echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
