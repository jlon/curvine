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

//! HDFS FUSE operations integration tests
//!
//! This test file verifies that common file operations work correctly through FUSE:
//! - touch: Create empty files
//! - echo: Write and append to files
//! - vim: Edit files (read, write, truncate)
//! - cat: Read files
//! - ls: List directories
//!
//! ## Usage
//!
//! ```bash
//! export HDFS_NAMENODE=hdfs://dg-test-hdfs3
//! cargo test --features opendal-hdfs,jni -p curvine-ufs --test hdfs_fuse_operations_test -- --ignored
//! ```

#[cfg(any(feature = "opendal-hdfs", feature = "opendal-webhdfs"))]
mod fuse_operations_tests {
    use curvine_common::fs::{FileSystem, Path, Reader, Writer};
    use curvine_ufs::opendal::OpendalFileSystem;
    use std::collections::HashMap;
    use std::env;

    /// Test touch operation (create empty file)
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-hdfs")]
    fn test_touch_operation() {
        println!("Testing touch operation (create empty file)");

        let namenode = env::var("HDFS_NAMENODE")
            .unwrap_or_else(|_| "hdfs://dg-test-hdfs3".to_string());
        println!("HDFS Namenode: {}", namenode);

        #[cfg(feature = "jni")]
        {
            use curvine_ufs::jni::{register_jvm, JVM};
            register_jvm();
            JVM.get_or_init().expect("Failed to initialize JVM");
            println!("JVM initialized");
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut conf = HashMap::new();
            conf.insert("hdfs.namenode".to_string(), namenode.clone());
            conf.insert("hdfs.root".to_string(), "/curvine-test".to_string());

            let current_user = env::var("USER").unwrap_or_else(|_| "hdfs".to_string());
            conf.insert("hdfs.user".to_string(), current_user.clone());
            println!("Using HDFS user: {}", current_user);

            let path = Path::from_str(&format!("{}/curvine-test/touch_test.txt", namenode)).unwrap();
            let fs = OpendalFileSystem::new(&path, conf).expect("Failed to create HDFS filesystem");

            // Test 1: Create empty file (touch)
            println!("Test 1: Creating empty file (touch operation)");
            match fs.create(&path, false).await {
                Ok(mut writer) => {
                    // Complete without writing any data (simulating touch)
                    match writer.complete().await {
                        Ok(_) => {
                            println!("✓ Empty file created successfully");
                            
                            // Verify file exists and is empty
                            match fs.get_status(&path).await {
                                Ok(status) => {
                                    assert_eq!(status.len, 0, "File should be empty");
                                    assert!(!status.is_dir, "Should be a file, not directory");
                                    println!("✓ File exists and is empty (size: {} bytes)", status.len);
                                }
                                Err(e) => panic!("Failed to verify empty file: {}", e),
                            }
                        }
                        Err(e) => panic!("Failed to complete empty file creation: {}", e),
                    }
                }
                Err(e) => panic!("Failed to create empty file: {}", e),
            }

            // Cleanup
            let _ = fs.delete(&path, false).await;
            println!("Touch operation test completed!");
        });
    }

    /// Test echo operation (write and append)
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-hdfs")]
    fn test_echo_operations() {
        println!("Testing echo operations (write and append)");

        let namenode = env::var("HDFS_NAMENODE")
            .unwrap_or_else(|_| "hdfs://dg-test-hdfs3".to_string());

        #[cfg(feature = "jni")]
        {
            use curvine_ufs::jni::{register_jvm, JVM};
            register_jvm();
            JVM.get_or_init().expect("Failed to initialize JVM");
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut conf = HashMap::new();
            conf.insert("hdfs.namenode".to_string(), namenode.clone());
            conf.insert("hdfs.root".to_string(), "/curvine-test".to_string());

            let current_user = env::var("USER").unwrap_or_else(|_| "hdfs".to_string());
            conf.insert("hdfs.user".to_string(), current_user.clone());
            println!("Using HDFS user: {}", current_user);

            let path = Path::from_str(&format!("{}/curvine-test/echo_test.txt", namenode)).unwrap();
            let fs = OpendalFileSystem::new(&path, conf).expect("Failed to create HDFS filesystem");

            // Test 1: Write operation (echo "hello" > file.txt)
            println!("Test 1: Writing to file (echo 'hello' > file.txt)");
            let write_content = b"hello\n";
            match fs.create(&path, true).await {
                Ok(mut writer) => {
                    match writer.write(write_content).await {
                        Ok(_) => {
                            match writer.complete().await {
                                Ok(_) => {
                                    println!("✓ File written successfully");
                                    
                                    // Verify content
                                    match fs.open(&path).await {
                                        Ok(mut reader) => {
                                            let mut buffer = vec![0; write_content.len()];
                                            match reader.read(&mut buffer).await {
                                                Ok(bytes_read) => {
                                                    assert_eq!(bytes_read, write_content.len());
                                                    assert_eq!(buffer, write_content);
                                                    println!("✓ File content verified: {}", String::from_utf8_lossy(&buffer));
                                                }
                                                Err(e) => panic!("Failed to read file: {}", e),
                                            }
                                        }
                                        Err(e) => panic!("Failed to open file for reading: {}", e),
                                    }
                                }
                                Err(e) => panic!("Failed to complete write: {}", e),
                            }
                        }
                        Err(e) => panic!("Failed to write data: {}", e),
                    }
                }
                Err(e) => panic!("Failed to create file for writing: {}", e),
            }

            // Test 2: Append operation (echo "world" >> file.txt)
            println!("Test 2: Appending to file (echo 'world' >> file.txt)");
            let append_content = b"world\n";
            match fs.append(&path).await {
                Ok(mut writer) => {
                    match writer.write(append_content).await {
                        Ok(_) => {
                            match writer.complete().await {
                                Ok(_) => {
                                    println!("✓ File appended successfully");
                                    
                                    // Verify appended content
                                    match fs.open(&path).await {
                                        Ok(mut reader) => {
                                            let expected_len = write_content.len() + append_content.len();
                                            let mut buffer = vec![0; expected_len];
                                            match reader.read(&mut buffer).await {
                                                Ok(bytes_read) => {
                                                    assert_eq!(bytes_read, expected_len);
                                                    let expected_content: Vec<u8> = [write_content.as_slice(), append_content.as_slice()].concat();
                                                    assert_eq!(buffer, expected_content);
                                                    println!("✓ Appended content verified: {}", String::from_utf8_lossy(&buffer));
                                                }
                                                Err(e) => panic!("Failed to read appended file: {}", e),
                                            }
                                        }
                                        Err(e) => panic!("Failed to open appended file: {}", e),
                                    }
                                }
                                Err(e) => panic!("Failed to complete append: {}", e),
                            }
                        }
                        Err(e) => panic!("Failed to append data: {}", e),
                    }
                }
                Err(e) => panic!("Failed to create append writer: {}", e),
            }

            // Cleanup
            let _ = fs.delete(&path, false).await;
            println!("Echo operations test completed!");
        });
    }

    /// Test vim-like operations (read, write, truncate)
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-hdfs")]
    fn test_vim_operations() {
        println!("Testing vim-like operations (read, write, truncate)");

        let namenode = env::var("HDFS_NAMENODE")
            .unwrap_or_else(|_| "hdfs://dg-test-hdfs3".to_string());

        #[cfg(feature = "jni")]
        {
            use curvine_ufs::jni::{register_jvm, JVM};
            register_jvm();
            JVM.get_or_init().expect("Failed to initialize JVM");
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut conf = HashMap::new();
            conf.insert("hdfs.namenode".to_string(), namenode.clone());
            conf.insert("hdfs.root".to_string(), "/curvine-test".to_string());

            let current_user = env::var("USER").unwrap_or_else(|_| "hdfs".to_string());
            conf.insert("hdfs.user".to_string(), current_user.clone());
            println!("Using HDFS user: {}", current_user);

            let path = Path::from_str(&format!("{}/curvine-test/vim_test.txt", namenode)).unwrap();
            let fs = OpendalFileSystem::new(&path, conf).expect("Failed to create HDFS filesystem");

            // Step 1: Create initial file with content
            println!("Step 1: Creating initial file with content");
            let initial_content = b"initial content\nline 2\nline 3\n";
            match fs.create(&path, true).await {
                Ok(mut writer) => {
                    writer.write(initial_content).await.unwrap();
                    writer.complete().await.unwrap();
                    println!("✓ Initial file created");
                }
                Err(e) => panic!("Failed to create initial file: {}", e),
            }

            // Step 2: Read file (vim opens file)
            println!("Step 2: Reading file (vim opens file)");
            match fs.open(&path).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; initial_content.len()];
                    match reader.read(&mut buffer).await {
                        Ok(bytes_read) => {
                            assert_eq!(bytes_read, initial_content.len());
                            assert_eq!(buffer, initial_content);
                            println!("✓ File read successfully: {}", String::from_utf8_lossy(&buffer));
                        }
                        Err(e) => panic!("Failed to read file: {}", e),
                    }
                }
                Err(e) => panic!("Failed to open file for reading: {}", e),
            }

            // Step 3: Truncate and rewrite (vim saves file with O_TRUNC)
            println!("Step 3: Truncate and rewrite (vim saves file)");
            let new_content = b"new content\nedited line\n";
            match fs.create(&path, true).await {
                Ok(mut writer) => {
                    // Overwrite should truncate the file
                    match writer.write(new_content).await {
                        Ok(_) => {
                            match writer.complete().await {
                                Ok(_) => {
                                    println!("✓ File truncated and rewritten");
                                    
                                    // Verify new content
                                    match fs.open(&path).await {
                                        Ok(mut reader) => {
                                            let mut buffer = vec![0; new_content.len()];
                                            match reader.read(&mut buffer).await {
                                                Ok(bytes_read) => {
                                                    assert_eq!(bytes_read, new_content.len());
                                                    assert_eq!(buffer, new_content);
                                                    println!("✓ New content verified: {}", String::from_utf8_lossy(&buffer));
                                                    
                                                    // Verify file size changed
                                                    match fs.get_status(&path).await {
                                                        Ok(status) => {
                                                            assert_eq!(status.len, new_content.len() as i64);
                                                            assert_ne!(status.len, initial_content.len() as i64);
                                                            println!("✓ File size updated correctly: {} bytes", status.len);
                                                        }
                                                        Err(e) => panic!("Failed to get file status: {}", e),
                                                    }
                                                }
                                                Err(e) => panic!("Failed to read new content: {}", e),
                                            }
                                        }
                                        Err(e) => panic!("Failed to open file after truncate: {}", e),
                                    }
                                }
                                Err(e) => panic!("Failed to complete truncate write: {}", e),
                            }
                        }
                        Err(e) => panic!("Failed to write new content: {}", e),
                    }
                }
                Err(e) => panic!("Failed to create file for truncate: {}", e),
            }

            // Cleanup
            let _ = fs.delete(&path, false).await;
            println!("Vim operations test completed!");
        });
    }

    /// Test seek operation (for random writes)
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-hdfs")]
    fn test_seek_operation() {
        println!("Testing seek operation");

        let namenode = env::var("HDFS_NAMENODE")
            .unwrap_or_else(|_| "hdfs://dg-test-hdfs3".to_string());

        #[cfg(feature = "jni")]
        {
            use curvine_ufs::jni::{register_jvm, JVM};
            register_jvm();
            JVM.get_or_init().expect("Failed to initialize JVM");
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut conf = HashMap::new();
            conf.insert("hdfs.namenode".to_string(), namenode.clone());
            conf.insert("hdfs.root".to_string(), "/curvine-test".to_string());

            let current_user = env::var("USER").unwrap_or_else(|_| "hdfs".to_string());
            conf.insert("hdfs.user".to_string(), current_user.clone());
            println!("Using HDFS user: {}", current_user);

            let path = Path::from_str(&format!("{}/curvine-test/seek_test.txt", namenode)).unwrap();
            let fs = OpendalFileSystem::new(&path, conf).expect("Failed to create HDFS filesystem");

            // Create file with initial content
            let initial_content = b"0123456789";
            match fs.create(&path, true).await {
                Ok(mut writer) => {
                    writer.write(initial_content).await.unwrap();
                    writer.complete().await.unwrap();
                    println!("✓ Initial file created");
                }
                Err(e) => panic!("Failed to create file: {}", e),
            }

            // Test seek forward
            println!("Test: Seeking forward and writing");
            match fs.create(&path, false).await {
                Ok(mut writer) => {
                    // Seek to position 5
                    match writer.seek(5).await {
                        Ok(_) => {
                            println!("✓ Seek to position 5 successful");
                            
                            // Write new content at position 5
                            let new_content = b"abc";
                            match writer.write(new_content).await {
                                Ok(_) => {
                                    match writer.complete().await {
                                        Ok(_) => {
                                            println!("✓ Write at seek position successful");
                                            
                                            // Verify content
                                            // Initial: "0123456789" (10 bytes)
                                            // Seek to pos 5, write "abc" (3 bytes)
                                            // Expected: "01234abc89" (10 bytes) - overwrites bytes 5-7 (567 -> abc), keeps 89
                                            match fs.open(&path).await {
                                                Ok(mut reader) => {
                                                    let mut buffer = vec![0; 12];
                                                    match reader.read(&mut buffer).await {
                                                        Ok(bytes_read) => {
                                                            let expected = b"01234abc89";
                                                            assert_eq!(bytes_read, expected.len(), "File size should be {} bytes", expected.len());
                                                            assert_eq!(&buffer[..bytes_read], expected);
                                                            println!("✓ Seek and write verified: {}", String::from_utf8_lossy(&buffer[..bytes_read]));
                                                        }
                                                        Err(e) => panic!("Failed to read after seek: {}", e),
                                                    }
                                                }
                                                Err(e) => panic!("Failed to open file after seek: {}", e),
                                            }
                                        }
                                        Err(e) => panic!("Failed to complete after seek: {}", e),
                                    }
                                }
                                Err(e) => panic!("Failed to write after seek: {}", e),
                            }
                        }
                        Err(e) => panic!("Failed to seek: {}", e),
                    }
                }
                Err(e) => panic!("Failed to create writer for seek: {}", e),
            }

            // Cleanup
            let _ = fs.delete(&path, false).await;
            println!("Seek operation test completed!");
        });
    }

    /// Comprehensive test: All operations together
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-hdfs")]
    fn test_comprehensive_fuse_operations() {
        println!("Testing comprehensive FUSE operations");

        let namenode = env::var("HDFS_NAMENODE")
            .unwrap_or_else(|_| "hdfs://dg-test-hdfs3".to_string());

        #[cfg(feature = "jni")]
        {
            use curvine_ufs::jni::{register_jvm, JVM};
            register_jvm();
            JVM.get_or_init().expect("Failed to initialize JVM");
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut conf = HashMap::new();
            conf.insert("hdfs.namenode".to_string(), namenode.clone());
            conf.insert("hdfs.root".to_string(), "/curvine-test".to_string());

            let current_user = env::var("USER").unwrap_or_else(|_| "hdfs".to_string());
            conf.insert("hdfs.user".to_string(), current_user.clone());
            println!("Using HDFS user: {}", current_user);

            let test_dir = Path::from_str(&format!("{}/curvine-test/fuse_ops_test", namenode)).unwrap();
            let fs = OpendalFileSystem::new(&test_dir, conf.clone()).expect("Failed to create HDFS filesystem");

            // Create test directory
            println!("Creating test directory");
            let _ = fs.mkdir(&test_dir, true).await;

            // Test Case 1: touch file1.txt
            println!("\n=== Test Case 1: touch file1.txt ===");
            let file1 = Path::from_str(&format!("{}/curvine-test/fuse_ops_test/file1.txt", namenode)).unwrap();
            let fs1 = OpendalFileSystem::new(&file1, conf.clone()).expect("Failed to create filesystem");
            match fs1.create(&file1, false).await {
                Ok(mut writer) => {
                    writer.complete().await.unwrap();
                    println!("✓ touch file1.txt: SUCCESS");
                }
                Err(e) => panic!("touch file1.txt: FAILED - {}", e),
            }

            // Test Case 2: echo "hello" > file2.txt
            println!("\n=== Test Case 2: echo 'hello' > file2.txt ===");
            let file2 = Path::from_str(&format!("{}/curvine-test/fuse_ops_test/file2.txt", namenode)).unwrap();
            let fs2 = OpendalFileSystem::new(&file2, conf.clone()).expect("Failed to create filesystem");
            match fs2.create(&file2, true).await {
                Ok(mut writer) => {
                    writer.write(b"hello\n").await.unwrap();
                    writer.complete().await.unwrap();
                    println!("✓ echo 'hello' > file2.txt: SUCCESS");
                }
                Err(e) => panic!("echo 'hello' > file2.txt: FAILED - {}", e),
            }

            // Test Case 3: echo "world" >> file2.txt
            println!("\n=== Test Case 3: echo 'world' >> file2.txt ===");
            match fs2.append(&file2).await {
                Ok(mut writer) => {
                    writer.write(b"world\n").await.unwrap();
                    writer.complete().await.unwrap();
                    println!("✓ echo 'world' >> file2.txt: SUCCESS");
                }
                Err(e) => panic!("echo 'world' >> file2.txt: FAILED - {}", e),
            }

            // Test Case 4: cat file2.txt (read)
            println!("\n=== Test Case 4: cat file2.txt ===");
            match fs2.open(&file2).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; 12]; // "hello\nworld\n"
                    match reader.read(&mut buffer).await {
                        Ok(bytes_read) => {
                            let content = String::from_utf8_lossy(&buffer[..bytes_read]);
                            println!("✓ cat file2.txt: SUCCESS - Content: {}", content);
                            assert!(content.contains("hello"));
                            assert!(content.contains("world"));
                        }
                        Err(e) => panic!("cat file2.txt: FAILED - {}", e),
                    }
                }
                Err(e) => panic!("cat file2.txt: FAILED - {}", e),
            }

            // Test Case 5: vim edit (truncate and rewrite)
            println!("\n=== Test Case 5: vim edit (truncate and rewrite) ===");
            match fs2.create(&file2, true).await {
                Ok(mut writer) => {
                    writer.write(b"edited content\n").await.unwrap();
                    writer.complete().await.unwrap();
                    println!("✓ vim edit: SUCCESS");
                }
                Err(e) => panic!("vim edit: FAILED - {}", e),
            }

            // Verify vim edit
            match fs2.open(&file2).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; 16]; // "edited content\n"
                    match reader.read(&mut buffer).await {
                        Ok(bytes_read) => {
                            let content = String::from_utf8_lossy(&buffer[..bytes_read]);
                            println!("✓ vim edit verified: Content: {}", content);
                            assert_eq!(content, "edited content\n");
                        }
                        Err(e) => panic!("vim edit verification: FAILED - {}", e),
                    }
                }
                Err(e) => panic!("vim edit verification: FAILED - {}", e),
            }

            // Test Case 6: ls (list directory)
            println!("\n=== Test Case 6: ls (list directory) ===");
            match fs.list_status(&test_dir).await {
                Ok(files) => {
                    println!("✓ ls: SUCCESS - Found {} files", files.len());
                    for file in &files {
                        println!("  - {} ({} bytes)", file.name, file.len);
                    }
                    assert!(files.len() >= 2, "Should have at least 2 files");
                }
                Err(e) => panic!("ls: FAILED - {}", e),
            }

            // Cleanup
            println!("\n=== Cleanup ===");
            let _ = fs.delete(&test_dir, true).await;
            println!("✓ Cleanup completed");

            println!("\n=== All FUSE operations tests completed successfully! ===");
        });
    }
}

