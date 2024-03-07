// Copyright (c) RoochNetwork
// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Error;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Hash, PartialEq)]
pub enum Code {
    Ok = 0,
    NotFound = 1,
    Corruption = 2,
    NotSupported = 3,
    InvalidArgument = 4,
    IoError = 5,
    MergeInProgress = 6,
    Incomplete = 7,
    ShutdownInProgress = 8,
    TimedOut = 9,
    Aborted = 10,
    Busy = 11,
    Expired = 12,
    TryAgain = 13,
    CompactionTooLarge = 14,
    ColumnFamilyDropped = 15,
}
#[derive(thiserror::Error, Debug)]
pub enum RawStoreError {
    #[error("store check error {0:?}")]
    StoreCheckError(Error),
    // Engine uses plain string as the error.
    #[error("store engine {0:?}")]
    Engine(#[from] Status),
    // TODO add more error types
}

impl From<RawStoreError> for String {
    fn from(e: RawStoreError) -> String {
        format!("{:?}", e)
    }
}

#[repr(C)]
#[derive(thiserror::Error, Debug)]
#[error("[{:?}] {}", .code, .state)]
pub struct Status {
    code: Code,
    state: String,
}

impl Status {
    pub fn with_error(code: Code, error: impl Into<String>) -> Self {
        Self {
            code,
            state: error.into(),
        }
    }
}
