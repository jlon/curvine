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

//! Write counter wrapper for tracking bytes written

#![allow(dead_code)]
use std::io::Write;

/// A wrapper around a writer that counts bytes written
pub struct WriteCounter<W> {
    inner: W,
    count: usize,
}

impl<W> WriteCounter<W>
where
    W: Write,
{
    /// Create a new WriteCounter wrapping the given writer
    pub fn new(inner: W) -> Self {
        WriteCounter { inner, count: 0 }
    }

    /// Consume the counter and return the inner writer
    pub fn into_inner(self) -> W {
        self.inner
    }

    /// Get the number of bytes written so far
    pub fn bytes_written(&self) -> usize {
        self.count
    }
}

impl<W> Write for WriteCounter<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let res = self.inner.write(buf);
        if let Ok(size) = res {
            self.count += size
        }
        res
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
