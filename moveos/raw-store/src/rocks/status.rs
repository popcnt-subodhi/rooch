// Copyright (c) RoochNetwork
// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::{Code, RawStoreError, Status};

/// A function that will transform a rocksdb error to Error.
///
/// r stands for rocksdb, e stands for engine_trait.
pub fn r2e(msg: impl Into<String>) -> anyhow::Error {
    // TODO: use correct code.
    RawStoreError::Engine(Status::with_error(Code::IoError, msg)).into()
}

/// A function that will transform an engine trait error to rocksdb error.
///
/// r stands for rocksdb, e stands for engine_trait.
#[allow(dead_code)]
pub fn e2r(s: RawStoreError) -> String {
    format!("{:?}", s)
}
