use curvine_config::{ClusterConf, JournalConf, MasterConf};
use curvine_model::{RenameFlags, WorkerInfo};
use curvine_runtime::common::Utils;
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::Master;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const PREFILL_COUNT: usize = 2048;
const DEFAULT_READ_FILE_COUNT: usize = 16_384;

#[derive(Clone, Copy)]
enum EdgeOperation {
    Rename,
    MkdirDelete,
    Link,
    Symlink,
    DeleteRecreate,
}

#[derive(Clone, Copy)]
enum ParentLayout {
    SameParent,
    DistinctParents,
}

#[derive(Clone, Copy)]
enum MetadataReadOperation {
    FileStatus,
    EmptyFileBlockLocations,
}

impl MetadataReadOperation {
    fn name(self) -> &'static str {
        match self {
            Self::FileStatus => "file_status",
            Self::EmptyFileBlockLocations => "empty_file_block_locations",
        }
    }
}

impl ParentLayout {
    fn name(self) -> &'static str {
        match self {
            Self::SameParent => "same_parent",
            Self::DistinctParents => "distinct_parents",
        }
    }

    fn directory(self, worker: usize) -> String {
        match self {
            Self::SameParent => "/bench/hot".to_string(),
            Self::DistinctParents => format!("/bench/parent-{worker:02}/hot"),
        }
    }
}

impl EdgeOperation {
    fn name(self) -> &'static str {
        match self {
            Self::Rename => "rename",
            Self::MkdirDelete => "mkdir_delete_cycle",
            Self::Link => "link_delete_cycle",
            Self::Symlink => "symlink_delete_cycle",
            Self::DeleteRecreate => "delete_recreate_cycle",
        }
    }
}

fn test_dir(name: &str) -> PathBuf {
    let mut path = PathBuf::from(
        std::env::var("CURVINE_METADATA_BENCH_DIR").unwrap_or_else(|_| "/dev/shm".to_string()),
    );
    path.push(format!(
        "curvine-metadata-edge-qps-{name}-{}",
        Utils::rand_str(8)
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn new_fs(name: &str) -> (MasterFilesystem, JournalSystem, Vec<PathBuf>) {
    Master::init_test_metrics();
    let meta_dir = test_dir(&format!("meta-{name}"));
    let journal_dir = test_dir(&format!("journal-{name}"));
    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: meta_dir.display().to_string(),
            // The benchmark invokes MasterFilesystem directly; RPC worker
            // pools do not execute the measured operations.
            io_threads: 2,
            worker_threads: 2,
            actor_threads: 1,
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: journal_dir.display().to_string(),
            io_threads: 2,
            worker_threads: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let js = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &js);
    fs.add_test_worker(WorkerInfo::default());
    (fs, js, vec![meta_dir, journal_dir])
}

fn cleanup_test_dirs(dirs: Vec<PathBuf>) {
    for dir in dirs {
        std::fs::remove_dir_all(dir).unwrap();
    }
}

fn assert_mem_store_consistent(fs: &MasterFilesystem) {
    let fs_dir = fs.fs_dir.read();
    assert_eq!(
        fs_dir.root_dir().sum_hash().unwrap(),
        fs_dir.create_tree().unwrap().sum_hash().unwrap()
    );
}

fn prepare_hot_directories(fs: &MasterFilesystem, layout: ParentLayout, threads: usize) {
    let prefill_count = match layout {
        ParentLayout::SameParent => PREFILL_COUNT,
        ParentLayout::DistinctParents => PREFILL_COUNT.div_ceil(threads),
    };
    if matches!(layout, ParentLayout::SameParent) {
        let directory = layout.directory(0);
        fs.mkdir(&directory, true).unwrap();
        for index in 0..prefill_count {
            fs.create(format!("{directory}/prefill-{index:04}"), false)
                .unwrap();
        }
    }

    for worker in 0..threads {
        let directory = layout.directory(worker);
        if matches!(layout, ParentLayout::DistinctParents) {
            fs.mkdir(&directory, true).unwrap();
            for index in 0..prefill_count {
                fs.create(format!("{directory}/prefill-{index:04}"), false)
                    .unwrap();
            }
        }
        fs.create(format!("{directory}/source-{worker:02}"), false)
            .unwrap();
        fs.create(format!("{directory}/cycle-{worker:02}"), false)
            .unwrap();
    }
}

fn measure(operation: EdgeOperation, layout: ParentLayout, threads: usize, duration: Duration) {
    let (fs, js, dirs) = new_fs(&format!("{}-{}", operation.name(), layout.name()));
    prepare_hot_directories(&fs, layout, threads);
    let fs = Arc::new(fs);
    let start = Arc::new(Barrier::new(threads));
    let mut handles = Vec::with_capacity(threads);

    for worker in 0..threads {
        let fs = fs.clone();
        let start = start.clone();
        handles.push(thread::spawn(move || {
            let directory = layout.directory(worker);
            let source = format!("{directory}/source-{worker:02}");
            let cycle = format!("{directory}/cycle-{worker:02}");
            let mut index = 0u64;
            let mut renamed = false;
            start.wait();
            let begin = Instant::now();
            while begin.elapsed() < duration {
                match operation {
                    EdgeOperation::Rename => {
                        let target = if renamed {
                            format!("{directory}/rename-{worker:02}.a")
                        } else {
                            format!("{directory}/rename-{worker:02}.b")
                        };
                        let current = if renamed {
                            format!("{directory}/rename-{worker:02}.b")
                        } else {
                            format!("{directory}/rename-{worker:02}.a")
                        };
                        if index == 0 {
                            fs.create(&current, false).unwrap();
                        }
                        fs.rename(current, target, RenameFlags::empty()).unwrap();
                        renamed = !renamed;
                    }
                    EdgeOperation::MkdirDelete => {
                        let target = format!("{directory}/directory-{worker:02}");
                        fs.mkdir(&target, false).unwrap();
                        fs.delete(target, false).unwrap();
                    }
                    EdgeOperation::Link => {
                        let link = format!("{directory}/link-{worker:02}");
                        fs.link(&source, &link).unwrap();
                        fs.delete(link, false).unwrap();
                    }
                    EdgeOperation::Symlink => {
                        let link = format!("{directory}/symlink-{worker:02}");
                        fs.symlink("target", &link, false, 0o777).unwrap();
                        fs.delete(link, false).unwrap();
                    }
                    EdgeOperation::DeleteRecreate => {
                        fs.delete(&cycle, false).unwrap();
                        fs.create(&cycle, false).unwrap();
                    }
                }
                index += 1;
            }
            (index, begin.elapsed())
        }));
    }

    let mut operations = 0u64;
    let mut elapsed = Duration::ZERO;
    for handle in handles {
        let (count, took) = handle.join().unwrap();
        operations += count;
        elapsed = elapsed.max(took);
    }
    assert_mem_store_consistent(&fs);
    let qps = operations as f64 / elapsed.as_secs_f64();
    let rpc_qps = if matches!(
        operation,
        EdgeOperation::MkdirDelete
            | EdgeOperation::Link
            | EdgeOperation::Symlink
            | EdgeOperation::DeleteRecreate
    ) {
        qps * 2.0
    } else {
        qps
    };
    println!(
        "METADATA_EDGE_QPS layout={} operation={} operations={} elapsed_ms={} qps={:.2} rpc_qps={:.2}",
        layout.name(),
        operation.name(),
        operations,
        elapsed.as_millis(),
        qps,
        rpc_qps
    );
    drop(fs);
    drop(js);
    cleanup_test_dirs(dirs);
}

fn measure_directory_status_qps(writers: usize, readers: usize, duration: Duration) {
    let (fs, js, dirs) = new_fs("directory-status");
    prepare_hot_directories(&fs, ParentLayout::SameParent, writers);

    let min_children = (PREFILL_COUNT + writers * 2) as i32;
    let max_children = min_children + writers as i32;
    let fs = Arc::new(fs);
    let start = Arc::new(Barrier::new(writers + readers));
    let mut writer_handles = Vec::with_capacity(writers);
    let mut reader_handles = Vec::with_capacity(readers);

    for writer in 0..writers {
        let fs = fs.clone();
        let start = start.clone();
        writer_handles.push(thread::spawn(move || {
            let source = format!("/bench/hot/source-{writer:02}");
            let link = format!("/bench/hot/status-{writer:02}");
            let mut rpc_count = 0u64;
            start.wait();
            let begin = Instant::now();
            while begin.elapsed() < duration {
                fs.link(&source, &link).unwrap();
                fs.delete(&link, false).unwrap();
                rpc_count += 2;
            }
            (rpc_count, begin.elapsed())
        }));
    }

    for _ in 0..readers {
        let fs = fs.clone();
        let start = start.clone();
        reader_handles.push(thread::spawn(move || {
            let mut reads = 0u64;
            let mut errors = 0u64;
            let mut first_error = None;
            let mut samples = Vec::new();
            start.wait();
            let begin = Instant::now();
            while begin.elapsed() < duration {
                let sample = reads.is_multiple_of(256);
                let started = sample.then(Instant::now);
                match fs.file_status("/bench/hot") {
                    Ok(status) => assert!(
                        (min_children..=max_children).contains(&status.children_num),
                        "directory child count {} is outside [{}, {}]",
                        status.children_num,
                        min_children,
                        max_children
                    ),
                    Err(error) => {
                        errors += 1;
                        first_error.get_or_insert_with(|| error.to_string());
                    }
                }
                if let Some(started) = started {
                    samples.push(started.elapsed().as_micros() as u64);
                }
                reads += 1;
            }
            (reads, errors, first_error, samples, begin.elapsed())
        }));
    }

    let mut write_rpcs = 0u64;
    let mut elapsed = Duration::ZERO;
    for handle in writer_handles {
        let (count, took) = handle.join().unwrap();
        write_rpcs += count;
        elapsed = elapsed.max(took);
    }

    let mut reads = 0u64;
    let mut errors = 0u64;
    let mut first_error = None;
    let mut samples = Vec::new();
    for handle in reader_handles {
        let (count, failed, error, mut reader_samples, took) = handle.join().unwrap();
        reads += count;
        errors += failed;
        first_error = first_error.or(error);
        samples.append(&mut reader_samples);
        elapsed = elapsed.max(took);
    }

    assert_eq!(
        errors,
        0,
        "directory status reads failed during writes: {}",
        first_error.unwrap_or_default()
    );
    assert_mem_store_consistent(&fs);
    samples.sort_unstable();
    let p99 = samples
        .get(samples.len().saturating_sub(1) * 99 / 100)
        .copied()
        .unwrap_or_default();
    println!(
        "METADATA_DIRECTORY_STATUS_QPS writers={} readers={} write_rpcs={} write_rpc_qps={:.2} reads={} read_qps={:.2} sampled_p99_us={} samples={}",
        writers,
        readers,
        write_rpcs,
        write_rpcs as f64 / elapsed.as_secs_f64(),
        reads,
        reads as f64 / elapsed.as_secs_f64(),
        p99,
        samples.len(),
    );
    drop(fs);
    drop(js);
    cleanup_test_dirs(dirs);
}

fn measure_metadata_read_operation_qps(
    operation: MetadataReadOperation,
    files: usize,
    threads: usize,
    duration: Duration,
) {
    let (fs, js, dirs) = new_fs(operation.name());
    fs.mkdir("/bench/read", true).unwrap();
    let paths = (0..files)
        .map(|index| format!("/bench/read/file-{index:05}"))
        .collect::<Vec<_>>();
    for path in &paths {
        fs.create(path, false).unwrap();
    }

    let fs = Arc::new(fs);
    let paths = Arc::new(paths);
    let start = Arc::new(Barrier::new(threads));
    let mut handles = Vec::with_capacity(threads);
    for worker in 0..threads {
        let fs = fs.clone();
        let paths = paths.clone();
        let start = start.clone();
        handles.push(thread::spawn(move || {
            let mut reads = 0u64;
            let mut samples = Vec::new();
            start.wait();
            let begin = Instant::now();
            while begin.elapsed() < duration {
                let path = &paths[(reads as usize + worker) % paths.len()];
                let sample = reads.is_multiple_of(256);
                let started = sample.then(Instant::now);
                match operation {
                    MetadataReadOperation::FileStatus => {
                        assert!(fs.file_status(path).unwrap().id > 0);
                    }
                    MetadataReadOperation::EmptyFileBlockLocations => {
                        assert!(fs.get_block_locations(path).unwrap().block_locs.is_empty());
                    }
                }
                if let Some(started) = started {
                    samples.push(started.elapsed().as_micros() as u64);
                }
                reads += 1;
            }
            (reads, samples, begin.elapsed())
        }));
    }

    let mut reads = 0u64;
    let mut samples = Vec::new();
    let mut elapsed = Duration::ZERO;
    for handle in handles {
        let (count, mut thread_samples, took) = handle.join().unwrap();
        reads += count;
        samples.append(&mut thread_samples);
        elapsed = elapsed.max(took);
    }
    assert_mem_store_consistent(&fs);
    samples.sort_unstable();
    let p99 = samples
        .get(samples.len().saturating_sub(1) * 99 / 100)
        .copied()
        .unwrap_or_default();
    println!(
        "METADATA_READ_QPS operation={} files={} threads={} reads={} qps={:.2} sampled_p99_us={} samples={}",
        operation.name(),
        files,
        threads,
        reads,
        reads as f64 / elapsed.as_secs_f64(),
        p99,
        samples.len(),
    );
    drop(fs);
    drop(js);
    cleanup_test_dirs(dirs);
}

fn selected_operations() -> Vec<EdgeOperation> {
    let Some(workload) = std::env::var("CURVINE_METADATA_BENCH_WORKLOAD").ok() else {
        return vec![
            EdgeOperation::Rename,
            EdgeOperation::MkdirDelete,
            EdgeOperation::Link,
            EdgeOperation::Symlink,
            EdgeOperation::DeleteRecreate,
        ];
    };

    let operation = match workload.as_str() {
        "rename" => EdgeOperation::Rename,
        "mkdir_delete_cycle" => EdgeOperation::MkdirDelete,
        "link_delete_cycle" => EdgeOperation::Link,
        "symlink_delete_cycle" => EdgeOperation::Symlink,
        "delete_recreate_cycle" => EdgeOperation::DeleteRecreate,
        "directory_status" => return vec![],
        _ => panic!("unknown CURVINE_METADATA_BENCH_WORKLOAD: {workload}"),
    };
    vec![operation]
}

fn selected_parent_layout() -> ParentLayout {
    match std::env::var("CURVINE_METADATA_BENCH_LAYOUT").as_deref() {
        Ok("same_parent") | Err(_) => ParentLayout::SameParent,
        Ok("distinct_parents") => ParentLayout::DistinctParents,
        Ok(layout) => panic!("unknown CURVINE_METADATA_BENCH_LAYOUT: {layout}"),
    }
}

#[test]
#[ignore = "manual metadata QPS comparison; use CURVINE_METADATA_BENCH_WORKLOAD to isolate one workload per process"]
fn measure_hot_directory_edge_qps() {
    let seconds = std::env::var("CURVINE_METADATA_BENCH_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5);
    let threads = std::env::var("CURVINE_METADATA_BENCH_THREADS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16);
    assert!(seconds > 0);
    assert!(threads > 0);
    let duration = Duration::from_secs(seconds);
    let layout = selected_parent_layout();
    for operation in selected_operations() {
        measure(operation, layout, threads, duration);
    }

    let readers = std::env::var("CURVINE_METADATA_BENCH_STATUS_READERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    assert!(readers > 0);
    if std::env::var("CURVINE_METADATA_BENCH_WORKLOAD")
        .ok()
        .is_none_or(|workload| workload == "directory_status")
    {
        measure_directory_status_qps(threads, readers, duration);
    }
}

#[test]
#[ignore = "manual read-QPS comparison; use a working set above the thread-local cache capacity"]
fn measure_metadata_read_qps() {
    let seconds = std::env::var("CURVINE_METADATA_BENCH_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5);
    let threads = std::env::var("CURVINE_METADATA_BENCH_THREADS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16);
    let files = std::env::var("CURVINE_METADATA_BENCH_READ_FILES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_READ_FILE_COUNT);
    let operation = match std::env::var("CURVINE_METADATA_BENCH_READ_OPERATION").as_deref() {
        Ok("file_status") | Err(_) => MetadataReadOperation::FileStatus,
        Ok("empty_file_block_locations") => MetadataReadOperation::EmptyFileBlockLocations,
        Ok(operation) => panic!("unknown CURVINE_METADATA_BENCH_READ_OPERATION: {operation}"),
    };
    assert!(seconds > 0);
    assert!(threads > 0);
    assert!(
        files > 4096,
        "read working set must exceed the thread-local cache"
    );
    measure_metadata_read_operation_qps(operation, files, threads, Duration::from_secs(seconds));
}
