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

mod s3_fuse_operations_tests {
    use curvine_common::fs::{FileSystem, Path, Reader, Writer};
    use curvine_ufs::opendal::OpendalFileSystem;
    use std::collections::HashMap;
    use std::env;

    fn build_s3_conf() -> HashMap<String, String> {
        let mut conf = HashMap::new();
        let endpoint =
            env::var("S3_ENDPOINT_URL").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        let region = env::var("S3_REGION_NAME").unwrap_or_else(|_| "us-east-1".to_string());
        let access = env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "".to_string());
        let secret = env::var("S3_SECRET_KEY").unwrap_or_else(|_| "".to_string());

        conf.insert("s3.endpoint_url".to_string(), endpoint);
        conf.insert("s3.region_name".to_string(), region);
        if !access.is_empty() {
            conf.insert("s3.credentials.access".to_string(), access);
        }
        if !secret.is_empty() {
            conf.insert("s3.credentials.secret".to_string(), secret);
        }
        conf
    }

    fn s3_path(bucket: &str, key: &str) -> Path {
        Path::from_str(&format!("s3://{}/{}", bucket, key)).unwrap()
    }

    fn bucket_root() -> (String, String) {
        let bucket = env::var("S3_BUCKET").unwrap_or_else(|_| "flink".to_string());
        let root = env::var("S3_ROOT").unwrap_or_else(|_| "curvine-test".to_string());
        (bucket, root)
    }

    #[test]
    #[ignore]
    fn test_touch_operation_s3() {
        let (bucket, root) = bucket_root();
        let path = s3_path(&bucket, &format!("{}/touch_test.txt", root));
        let fs =
            OpendalFileSystem::new(&path, build_s3_conf()).expect("Failed to create S3 filesystem");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            match fs.create(&path, false).await {
                Ok(mut writer) => {
                    writer.complete().await.unwrap();
                    match fs.get_status(&path).await {
                        Ok(status) => {
                            assert_eq!(status.len, 0);
                            assert!(!status.is_dir);
                        }
                        Err(e) => panic!("Failed to verify empty file: {}", e),
                    }
                }
                Err(e) => panic!("Failed to create empty file: {}", e),
            }

            let _ = fs.delete(&path, false).await;
        });
    }

    #[test]
    #[ignore]
    fn test_echo_operations_s3() {
        let (bucket, root) = bucket_root();
        let path = s3_path(&bucket, &format!("{}/echo_test.txt", root));
        let fs =
            OpendalFileSystem::new(&path, build_s3_conf()).expect("Failed to create S3 filesystem");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let write_content = b"hello\n";
            match fs.create(&path, true).await {
                Ok(mut writer) => {
                    writer.write(write_content).await.unwrap();
                    writer.complete().await.unwrap();
                }
                Err(e) => panic!("Failed to create file for writing: {}", e),
            }

            let append_content = b"world\n";
            match fs.append(&path).await {
                Ok(mut writer) => {
                    writer.write(append_content).await.unwrap();
                    writer.complete().await.unwrap();
                }
                Err(e) => panic!("Failed to create append writer: {}", e),
            }

            match fs.open(&path).await {
                Ok(mut reader) => {
                    let expected_len = write_content.len() + append_content.len();
                    let mut buffer = vec![0; expected_len];
                    let bytes_read = reader.read(&mut buffer).await.unwrap();
                    assert_eq!(bytes_read, expected_len);
                    let expected: Vec<u8> =
                        [write_content.as_slice(), append_content.as_slice()].concat();
                    assert_eq!(buffer, expected);
                }
                Err(e) => panic!("Failed to open file for reading: {}", e),
            }

            let _ = fs.delete(&path, false).await;
        });
    }

    #[test]
    #[ignore]
    fn test_vim_operations_s3() {
        let (bucket, root) = bucket_root();
        let path = s3_path(&bucket, &format!("{}/vim_test.txt", root));
        let fs =
            OpendalFileSystem::new(&path, build_s3_conf()).expect("Failed to create S3 filesystem");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let initial_content = b"initial content\nline 2\nline 3\n";
            match fs.create(&path, true).await {
                Ok(mut writer) => {
                    writer.write(initial_content).await.unwrap();
                    writer.complete().await.unwrap();
                }
                Err(e) => panic!("Failed to create initial file: {}", e),
            }

            match fs.open(&path).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; initial_content.len()];
                    let bytes_read = reader.read(&mut buffer).await.unwrap();
                    assert_eq!(bytes_read, initial_content.len());
                    assert_eq!(buffer, initial_content);
                }
                Err(e) => panic!("Failed to open file: {}", e),
            }

            let new_content = b"new content\nedited line\n";
            match fs.create(&path, true).await {
                Ok(mut writer) => {
                    writer.write(new_content).await.unwrap();
                    writer.complete().await.unwrap();
                    match fs.get_status(&path).await {
                        Ok(status) => {
                            assert_eq!(status.len, new_content.len() as i64);
                        }
                        Err(e) => panic!("Failed to get file status: {}", e),
                    }
                }
                Err(e) => panic!("Failed to create file for truncate: {}", e),
            }

            let _ = fs.delete(&path, false).await;
        });
    }

    #[test]
    #[ignore]
    fn test_seek_operation_s3() {
        let (bucket, root) = bucket_root();
        let path = s3_path(&bucket, &format!("{}/seek_test.txt", root));
        let fs =
            OpendalFileSystem::new(&path, build_s3_conf()).expect("Failed to create S3 filesystem");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let initial_content = b"0123456789";
            match fs.create(&path, true).await {
                Ok(mut writer) => {
                    writer.write(initial_content).await.unwrap();
                    writer.complete().await.unwrap();
                }
                Err(e) => panic!("Failed to create file: {}", e),
            }

            match fs.create(&path, false).await {
                Ok(mut writer) => {
                    writer.seek(5).await.unwrap();
                    let new_content = b"abc";
                    writer.write(new_content).await.unwrap();
                    writer.complete().await.unwrap();
                }
                Err(e) => panic!("Failed to create writer for seek: {}", e),
            }

            match fs.open(&path).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; 12];
                    let bytes_read = reader.read(&mut buffer).await.unwrap();
                    let expected = b"01234abc89";
                    assert_eq!(bytes_read, expected.len());
                    assert_eq!(&buffer[..bytes_read], expected);
                }
                Err(e) => panic!("Failed to open file after seek: {}", e),
            }

            let _ = fs.delete(&path, false).await;
        });
    }

    #[test]
    #[ignore]
    fn test_comprehensive_fuse_operations_s3() {
        let (bucket, root) = bucket_root();
        let test_dir = s3_path(&bucket, &format!("{}/fuse_ops_test", root));
        let fs = OpendalFileSystem::new(&test_dir, build_s3_conf())
            .expect("Failed to create S3 filesystem");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _ = fs.mkdir(&test_dir, true).await;

            let file1 = s3_path(&bucket, &format!("{}/fuse_ops_test/file1.txt", root));
            let fs1 = OpendalFileSystem::new(&file1, build_s3_conf())
                .expect("Failed to create filesystem");
            match fs1.create(&file1, false).await {
                Ok(mut writer) => {
                    writer.complete().await.unwrap();
                }
                Err(e) => panic!("touch file1.txt: {}", e),
            }

            let file2 = s3_path(&bucket, &format!("{}/fuse_ops_test/file2.txt", root));
            let fs2 = OpendalFileSystem::new(&file2, build_s3_conf())
                .expect("Failed to create filesystem");
            match fs2.create(&file2, true).await {
                Ok(mut writer) => {
                    writer.write(b"hello\n").await.unwrap();
                    writer.complete().await.unwrap();
                }
                Err(e) => panic!("echo > file2.txt: {}", e),
            }

            match fs2.append(&file2).await {
                Ok(mut writer) => {
                    writer.write(b"world\n").await.unwrap();
                    writer.complete().await.unwrap();
                }
                Err(e) => panic!("echo >> file2.txt: {}", e),
            }

            match fs2.open(&file2).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; 12];
                    let bytes_read = reader.read(&mut buffer).await.unwrap();
                    let content = String::from_utf8_lossy(&buffer[..bytes_read]);
                    assert!(content.contains("hello"));
                    assert!(content.contains("world"));
                }
                Err(e) => panic!("cat file2.txt: {}", e),
            }

            match fs2.create(&file2, true).await {
                Ok(mut writer) => {
                    writer.write(b"edited content\n").await.unwrap();
                    writer.complete().await.unwrap();
                }
                Err(e) => panic!("vim edit: {}", e),
            }

            match fs2.open(&file2).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; 16];
                    let bytes_read = reader.read(&mut buffer).await.unwrap();
                    let content = String::from_utf8_lossy(&buffer[..bytes_read]);
                    assert_eq!(content, "edited content\n");
                }
                Err(e) => panic!("vim verify: {}", e),
            }

            match fs.list_status(&test_dir).await {
                Ok(files) => {
                    assert!(files.len() >= 2);
                }
                Err(e) => panic!("ls: {}", e),
            }

            let _ = fs.delete(&test_dir, true).await;
        });
    }
}
