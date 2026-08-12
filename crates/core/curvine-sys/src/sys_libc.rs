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

#![allow(unused, clippy::not_unsafe_ptr_arg_deref)]

use crate::*;
use fs2::FileExt;
use std::ffi::CString;
use std::fs;
use std::fs::Metadata;
use std::io::{ErrorKind, IoSlice};
use std::path::Path;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

// It defines the underlying functions of the operating system, which is an encapsulation of the libc library functions.
// The main supported systems are linux.

// Get the file descriptor of an io object (file, network), and for non-linux systems, an error is returned.
#[cfg(target_os = "linux")]
pub fn get_raw_io<T>(io: &T) -> SysResult<CInt>
where
    T: AsRawFd,
{
    Ok(io.as_raw_fd())
}

#[cfg(not(target_os = "linux"))]
pub fn get_raw_io<T>(_: &T) -> SysResult<CInt> {
    sys_error!(ErrorKind::Unsupported, "Unsupported os")
}

pub fn close(raw_io: RawIO) -> SysResult<()> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(
            ErrorKind::Unsupported,
            "Unsupported close raw id {}",
            raw_io
        )
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            sys_call!(libc::close(raw_io))?;
        }
        Ok(())
    }
}

pub fn close_raw_io(raw_io: RawIO) -> SysResult<()> {
    close(raw_io)
}

// Linux sendfile function to send the number of files to the network.
// C function prototype:
//pub unsafe extern "C" fn sendfile(
//     out_fd: c_int,
//     in_fd: c_int,
//     offset: *mut off_t,
//     count: size_t
// ) -> ssize_t
pub fn send_file(
    fd_in: RawIO,
    fd_out: RawIO,
    off: Option<&mut i64>,
    len: usize,
) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let off = match off {
            Some(v) => v as *mut _,
            None => std::ptr::null_mut(),
        };
        let res = unsafe { libc::sendfile(fd_out, fd_in, off, len as libc::size_t) };
        sys_call!(res)
    }
}

// Linux splice function to copy data between 2 FDs.
// C function prototype:
// pub unsafe extern "C" fn splice(
//     fd_in: c_int,
//     off_in: *mut loff_t,
//     fd_out: c_int,
//     off_out: *mut loff_t,
//     len: size_t,
//     flags: c_uint
// ) -> size_t
pub fn splice(
    fd_in: RawIO,
    off_in: Option<&mut i64>,
    fd_out: RawIO,
    off_out: Option<&mut i64>,
    len: usize,
) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let off_in = match off_in {
            Some(v) => v as *mut _,
            None => std::ptr::null_mut(),
        };
        let off_out = match off_out {
            Some(v) => v as *mut _,
            None => std::ptr::null_mut(),
        };

        let res = unsafe {
            libc::splice(
                fd_in,
                off_in,
                fd_out,
                off_out,
                len as libc::size_t,
                libc::SPLICE_F_NONBLOCK | libc::SPLICE_F_MOVE,
            )
        };
        sys_call!(res)
    }
}

pub fn splice_out_full(
    fd_in: RawIO,
    mut off_in: Option<i64>,
    fd_out: RawIO,
    mut off_out: Option<i64>,
    len: usize,
) -> SysResult<()> {
    let mut remaining = len;
    while remaining > 0 {
        let transferred = splice(fd_in, off_in.as_mut(), fd_out, off_out.as_mut(), remaining)?;
        if transferred == 0 {
            return sys_error!(ErrorKind::UnexpectedEof, "splice returned 0");
        }
        remaining -= transferred as usize;
    }

    Ok(())
}

// Return to whether the current pipeline is blocking mode
pub fn pipe_is_blocking(fd: RawIO) -> bool {
    #[cfg(not(target_os = "linux"))]
    {
        true
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            let status_flags = libc::fcntl(fd, libc::F_GETFL);
            status_flags & libc::O_NONBLOCK == 0
        }
    }
}

// Modify the pipeline blocking mode.
pub fn set_pipe_blocking(fd: RawIO, blocking: bool) -> SysResult<()> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            let status_flags = sys_call!(libc::fcntl(fd, libc::F_GETFL))?;
            let res = if blocking {
                libc::fcntl(fd, libc::F_SETFL, status_flags & !libc::O_NONBLOCK)
            } else {
                libc::fcntl(fd, libc::F_SETFL, status_flags | libc::O_NONBLOCK)
            };

            sys_call!(res)?;
            Ok(())
        }
    }
}

pub fn pipe2(size: usize) -> SysResult<[RawIO; 2]> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            let mut fds: [RawIO; 2] = [-1, -1];
            let res = libc::pipe2(
                fds.as_mut_ptr() as *mut libc::c_int,
                libc::O_CLOEXEC | libc::O_NONBLOCK,
            );
            sys_call!(res)?;

            let set_buf_res = libc::fcntl(fds[1], libc::F_SETPIPE_SZ, size);
            sys_call!(set_buf_res)?;

            let set_buf_res = libc::fcntl(fds[0], libc::F_SETPIPE_SZ, size);
            sys_call!(set_buf_res)?;

            if (set_buf_res as usize) < size {
                return sys_error!(
                    ErrorKind::InvalidInput,
                    "Failed to set pipe size, expected: {}, actual: {}",
                    size,
                    set_buf_res
                );
            }

            Ok(fds)
        }
    }
}

// Operating system pre-read api.
//pub unsafe extern "C" fn posix_fadvise(
//     fd: c_int,
//     offset: off_t,
//     len: off_t,
//     advise: c_int
// ) -> c_int
pub fn read_ahead(file: &std::fs::File, off: i64, len: i64) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        Ok(0)
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            let fd = get_raw_io(file)?;
            let res = libc::posix_fadvise(
                fd,
                off as libc::off_t,
                len as libc::off_t,
                libc::POSIX_FADV_WILLNEED,
            );
            // posix_fadvise reports failure by returning a positive errno directly instead
            // of returning -1 and setting errno, so `sys_call!` must not be used here.
            if res == 0 {
                Ok(0)
            } else {
                Err(std::io::Error::from_raw_os_error(res))
            }
        }
    }
}

pub fn is_tmpfs(file_path: &str) -> SysResult<bool> {
    #[cfg(not(target_os = "linux"))]
    {
        Ok(false)
    }

    #[cfg(target_os = "linux")]
    {
        let path = match std::ffi::CString::new(file_path) {
            Err(e) => return sys_error!(ErrorKind::InvalidInput, "CString::new {}", e),
            Ok(v) => v,
        };

        unsafe {
            let mut stat: libc::statfs = std::mem::zeroed();
            sys_call!(libc::statfs(path.as_ptr(), &mut stat))?;
            Ok(stat.f_type == libc::TMPFS_MAGIC)
        }
    }
}

pub fn thread_name() -> String {
    std::thread::current()
        .name()
        .unwrap_or("unknown")
        .to_string()
}

pub fn read(fd: RawIO, buf: &mut [u8]) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe {
            use libc::{self, c_void, size_t};
            libc::read(fd, buf.as_ptr() as *mut c_void, buf.len() as size_t)
        };

        sys_call!(res)
    }
}

pub fn read_full(fd: RawIO, buf: &mut [u8]) -> SysResult<()> {
    let mut remaining = buf.len();
    let mut off = 0;

    while remaining > 0 {
        let read_len = read(fd, &mut buf[off..])? as usize;
        remaining -= read_len;
        off += read_len;
    }

    Ok(())
}

pub fn writev(fd: RawIO, bufs: &[IoSlice<'_>]) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res =
            unsafe { libc::writev(fd, bufs.as_ptr() as *const libc::iovec, bufs.len() as CInt) };

        sys_call!(res)
    }
}

pub fn get_uid() -> u32 {
    #[cfg(target_os = "linux")]
    {
        unsafe { libc::geteuid() }
    }

    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

pub fn get_gid() -> u32 {
    #[cfg(target_os = "linux")]
    {
        unsafe { libc::getegid() }
    }

    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

//pub unsafe extern "C" fn vmsplice(
//     fd: c_int,
//     iov: *const iovec,
//     nr_segs: size_t,
//     flags: c_uint
// ) -> ssize_t
pub fn vm_splice(fd: RawIO, iov: &[IoSlice<'_>]) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe {
            libc::vmsplice(
                fd,
                iov.as_ptr() as *const libc::iovec,
                iov.len(),
                libc::SPLICE_F_NONBLOCK,
            )
        };

        sys_call!(res)
    }
}

// pub unsafe extern "C" fn open(
//     path: *const c_char,
//     oflag: c_int,
//     ...
// ) -> c_int
pub fn open(path: &CString, flag: i32) -> SysResult<RawIO> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::open(path.as_ptr(), flag | libc::O_CLOEXEC) };
        sys_call!(res)
    }
}

// pub unsafe extern "C" fn ioctl(
//     fd: c_int,
//     request: c_ulong,
//     ...
// ) -> c_int
pub fn ioctl(fd: RawIO, request: u64, arg: *mut libc::c_void) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::ioctl(fd, request, arg) };
        sys_call!(res)
    }
}

pub fn dup(fd: RawIO) -> SysResult<RawIO> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::dup(fd) };
        sys_call!(res)
    }
}

// Get the page size.
// pub unsafe extern "C" fn sysconf(name: c_int) -> c_long
pub fn get_pagesize() -> SysResult<usize> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        sys_call!(res)?;
        Ok(res as usize)
    }
}

pub fn get_device_id(path: &Path) -> u64 {
    if let Ok(metadata) = fs::metadata(path) {
        #[cfg(target_os = "linux")]
        {
            use std::os::linux::fs::MetadataExt;
            metadata.st_dev()
        }
        #[cfg(not(target_os = "linux"))]
        {
            0
        }
    } else {
        0
    }
}

/// Get UID by username with a reentrant libc lookup.
pub fn get_uid_by_name(username: &str) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        use nix::unistd::User;

        User::from_name(username)
            .ok()
            .flatten()
            .map(|user| user.uid.as_raw())
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Get username by UID with a reentrant libc lookup.
pub fn get_username_by_uid(uid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        use nix::unistd::{Uid, User};

        User::from_uid(Uid::from_raw(uid))
            .ok()
            .flatten()
            .map(|user| user.name)
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Get GID by group name with a reentrant libc lookup.
pub fn get_gid_by_name(groupname: &str) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        use nix::unistd::Group;

        Group::from_name(groupname)
            .ok()
            .flatten()
            .map(|group| group.gid.as_raw())
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Get group name by GID with a reentrant libc lookup.
pub fn get_groupname_by_gid(gid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        use nix::unistd::{Gid, Group};

        Group::from_gid(Gid::from_raw(gid))
            .ok()
            .flatten()
            .map(|group| group.name)
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// pub unsafe extern "C" fn ftruncate64(
//     fd: c_int,
//     length: off64_t,
// ) -> c_int
pub fn ftruncate(file: &fs::File, len: i64) -> SysResult<()> {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let fd = get_raw_io(file)?;
            let result = libc::ftruncate64(fd, len as libc::off64_t);

            sys_call!(result)?;
            Ok(())
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        file.set_len(len as u64)?;
        Ok(())
    }
}

/// pub unsafe extern "C" fn fallocate64(
//     fd: c_int,
//     mode: c_int,
//     offset: off64_t,
//     len: off64_t,
// ) -> c_int
pub fn fallocate(file: &fs::File, off: i64, len: i64, mode: i32) -> SysResult<()> {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let fd = get_raw_io(file)?;
            let result = libc::fallocate64(fd, mode, off as libc::off64_t, len as libc::off64_t);

            sys_call!(result)?;
            Ok(())
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        file.allocate(len as u64)?;
        Ok(())
    }
}

/// Get the actual size of the file
/// - If there are no holes in the file, returns the logical file size
/// - If there are holes, returns the actual disk space occupied
pub fn file_actual_size(metadata: Metadata) -> SysResult<u64> {
    // 4k
    // 1G
    let logical_size = metadata.len();

    #[cfg(target_os = "linux")]
    {
        use std::os::linux::fs::MetadataExt;
        let actual_size = metadata.st_blocks() * ST_BLOCK_SIZE;

        // If actual size is less than logical size, the file has holes
        // 1g > 4k
        if actual_size < logical_size {
            Ok(actual_size)
        } else {
            Ok(logical_size)
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        Ok(logical_size)
    }
}

pub fn fcntl_get(fd: RawIO) -> SysResult<CInt> {
    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        sys_call!(res)
    }

    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }
}

pub fn fcntl_set(fd: RawIO, flags: CInt) -> SysResult<CInt> {
    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::fcntl(fd, libc::F_SETFD, flags) };
        sys_call!(res)
    }

    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }
}

#[cfg(test)]
mod tests {
    use super::{get_gid_by_name, get_groupname_by_gid, get_uid_by_name, get_username_by_uid};

    #[cfg(target_os = "linux")]
    #[test]
    fn account_lookups_are_reentrant_under_concurrency() {
        let uid = unsafe { libc::geteuid() };
        let gid = unsafe { libc::getegid() };
        let username = get_username_by_uid(uid).expect("current uid must resolve");
        let groupname = get_groupname_by_gid(gid).expect("current gid must resolve");

        let mut users = vec![(username, uid)];
        if uid != 0 {
            if let Some(root_name) = get_username_by_uid(0) {
                users.push((root_name, 0));
            }
        }

        let mut groups = vec![(groupname, gid)];
        if gid != 0 {
            if let Some(root_group) = get_groupname_by_gid(0) {
                groups.push((root_group, 0));
            }
        }

        let workers = (0..16)
            .map(|_| {
                let users = users.clone();
                let groups = groups.clone();
                std::thread::spawn(move || {
                    for _ in 0..32 {
                        for (name, id) in &users {
                            assert_eq!(get_uid_by_name(name), Some(*id));
                            assert_eq!(get_username_by_uid(*id).as_deref(), Some(name.as_str()));
                        }
                        for (name, id) in &groups {
                            assert_eq!(get_gid_by_name(name), Some(*id));
                            assert_eq!(get_groupname_by_gid(*id).as_deref(), Some(name.as_str()));
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        for worker in workers {
            worker.join().expect("account lookup worker must not panic");
        }
    }
}
