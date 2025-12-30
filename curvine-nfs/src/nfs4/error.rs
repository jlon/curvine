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

//! NFSv4.1 Error Codes (RFC 5661)

use curvine_common::error::FsError;
use num_enum::{FromPrimitive, IntoPrimitive};
use std::fmt;
use tracing;

/// NFSv4.1 status codes
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum Nfs4Status {
    #[default]
    Ok = 0,

    // Generic errors
    Perm = 1,
    Noent = 2,
    Io = 5,
    Nxio = 6,
    Access = 13,
    Exist = 17,
    Xdev = 18,
    Notdir = 20,
    Isdir = 21,
    Inval = 22,
    Fbig = 27,
    Nospc = 28,
    Rofs = 30,
    Mlink = 31,
    Nametoolong = 63,
    Notempty = 66,
    Dquot = 69,
    Stale = 70,

    // NFSv4 specific errors
    Badhandle = 10001,
    BadCookie = 10003,
    Notsupp = 10004,
    Toosmall = 10005,
    Serverfault = 10006,
    Badtype = 10007,
    Delay = 10008,
    Same = 10009,
    Denied = 10010,
    Expired = 10011,
    Locked = 10012,
    Grace = 10013,
    Fhexpired = 10014,
    ShareDenied = 10015,
    Wrongsec = 10016,
    ClidInuse = 10017,
    Moved = 10019,
    Nofilehandle = 10020,
    MinorVersMismatch = 10021,
    StaleClientid = 10022,
    StaleStateid = 10023,
    OldStateid = 10024,
    BadStateid = 10025,
    BadSeqid = 10026,
    NotSame = 10027,
    LockRange = 10028,
    Symlink = 10029,
    Restorefh = 10030,
    LeaseMoved = 10031,
    AttrNotsupp = 10032,
    NoGrace = 10033,
    ReclaimBad = 10034,
    ReclaimConflict = 10035,
    BadXdr = 10036,
    LocksHeld = 10037,
    Openmode = 10038,
    BadOwner = 10039,
    Badchar = 10040,
    Badname = 10041,
    BadRange = 10042,
    LockNotsupp = 10043,
    OpIllegal = 10044,
    Deadlock = 10045,
    FileOpen = 10046,
    AdminRevoked = 10047,
    CbPathDown = 10048,

    // NFSv4.1 specific errors
    BadSession = 10052,
    BadSlot = 10053,
    CompleteAlready = 10054,
    ConnNotBoundToSession = 10055,
    DelegAlreadyWanted = 10056,
    BackChanBusy = 10057,
    LayoutTrylater = 10058,
    LayoutUnavailable = 10059,
    NomatchingLayout = 10060,
    RecallConflict = 10061,
    UnknownLayouttype = 10062,
    SeqMisordered = 10063,
    SequencePos = 10064,
    ReqTooBig = 10065,
    RepTooBig = 10066,
    RepTooBigToCache = 10067,
    RetryUncachedRep = 10068,
    UnsafeCompound = 10069,
    TooManyOps = 10070,
    OpNotInSession = 10071,
    HashAlgUnsupported = 10072,
    ClientidBusy = 10074,
    PnfsIoHole = 10075,
    SeqFalseRetry = 10076,
    BadHighSlot = 10077,
    DeadSession = 10078,
    EncrAlgUnsupported = 10079,
    PnfsNoLayout = 10080,
    NotOnlyOp = 10081,
    WrongCred = 10082,
    WrongType = 10083,
    DirdelegUnavail = 10084,
    RejectDeleg = 10085,
    ReturnConflict = 10086,
    DelegRevoked = 10087,
}

impl fmt::Display for Nfs4Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<&FsError> for Nfs4Status {
    fn from(e: &FsError) -> Self {
        match e {
            // File system errors
            FsError::FileNotFound(_) => Self::Noent,
            FsError::FileAlreadyExists(_) => Self::Exist,
            FsError::ParentNotDir(_) => Self::Notdir,
            FsError::DirNotEmpty(_) => Self::Notempty,
            FsError::InvalidPath(_) => Self::Inval,
            FsError::InvalidFileSize(_) => Self::Inval,

            // Storage errors
            FsError::DiskOutOfSpace(_) => Self::Nospc,
            FsError::BlockInfo(_) => Self::Io,
            FsError::BlockIsWriting(_) => Self::Delay,

            // Cluster errors
            FsError::NotLeaderMaster(_) => Self::Moved,
            FsError::Timeout(_) => Self::Delay,
            FsError::Lease(_) => Self::Expired,
            FsError::Expired(_) => Self::Expired,

            // Generic errors
            FsError::IO(_) => Self::Io,
            FsError::Unsupported(_) => Self::Notsupp,
            FsError::InProgress(_) => Self::Delay,

            // All other errors map to Serverfault
            _ => Self::Serverfault,
        }
    }
}

/// NFSv4.1 error type
#[derive(Debug)]
pub struct Nfs4Error {
    pub status: Nfs4Status,
    pub message: Option<String>,
    pub data: Option<Vec<u8>>,
}

impl Nfs4Error {
    pub fn new(status: Nfs4Status) -> Self {
        Self {
            status,
            message: None,
            data: None,
        }
    }

    pub fn with_message(status: Nfs4Status, message: impl Into<String>) -> Self {
        Self {
            status,
            message: Some(message.into()),
            data: None,
        }
    }

    pub fn with_data(status: Nfs4Status, data: Vec<u8>) -> Self {
        Self {
            status,
            message: None,
            data: Some(data),
        }
    }

    pub fn status(&self) -> Nfs4Status {
        self.status
    }
}

impl fmt::Display for Nfs4Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(msg) => write!(f, "{}: {}", self.status, msg),
            None => write!(f, "{}", self.status),
        }
    }
}

impl std::error::Error for Nfs4Error {}

impl From<Nfs4Status> for Nfs4Error {
    fn from(status: Nfs4Status) -> Self {
        Self::new(status)
    }
}

impl From<FsError> for Nfs4Error {
    fn from(e: FsError) -> Self {
        let status = Nfs4Status::from(&e);
        let msg = format!("[Curvine] {e}");

        // Log detailed error for server-side troubleshooting
        match status {
            Nfs4Status::Serverfault | Nfs4Status::Moved | Nfs4Status::Delay => {
                tracing::error!("Backend cluster error: {} -> NFS status: {:?}", msg, status);
            }
            Nfs4Status::Nospc => {
                tracing::warn!("Storage space exhausted: {}", msg);
            }
            _ => {}
        }

        Self::with_message(status, msg)
    }
}

impl From<std::io::Error> for Nfs4Error {
    fn from(e: std::io::Error) -> Self {
        Self::with_message(Nfs4Status::Io, e.to_string())
    }
}

impl From<anyhow::Error> for Nfs4Error {
    fn from(e: anyhow::Error) -> Self {
        Self::with_message(Nfs4Status::Serverfault, e.to_string())
    }
}

/// Result type for NFSv4.1 operations
pub type Nfs4Result<T> = Result<T, Nfs4Error>;
