// Copyright (c) RoochNetwork
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

pub mod batch;
mod status;

use crate::errors::RawStoreError;
use crate::metrics::{record_metrics, StoreMetrics};
use crate::rocks::batch::WriteBatch;
use crate::rocks::status::r2e;
use crate::traits::DBStore;
use crate::{ColumnFamilyName, WriteOp};
use anyhow::{ensure, format_err, Error, Result};
use moveos_common::utils::{check_open_fds_limit, from_bytes};
use moveos_config::store_config::RocksdbConfig;
use rocksdb::rocksdb_options::ColumnFamilyDescriptor;
use rocksdb::{
    CFHandle, ColumnFamilyOptions, DBCompressionType, DBIterator, DBOptions, ReadOptions, Writable,
    WriteBatch as DBWriteBatch, WriteOptions, DB,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

pub const DEFAULT_PREFIX_NAME: ColumnFamilyName = "default";
pub const RES_FDS: u64 = 4096;

#[allow(clippy::upper_case_acronyms)]
pub struct RocksDB {
    db: Arc<DB>,
    _cfs: Vec<ColumnFamilyName>,
    metrics: Option<StoreMetrics>,
}

impl RocksDB {
    pub fn new<P: AsRef<Path> + Clone>(
        db_path: P,
        column_families: Vec<ColumnFamilyName>,
        rocksdb_config: RocksdbConfig,
        metrics: Option<StoreMetrics>,
    ) -> Result<Self> {
        Self::open_with_cfs(db_path, column_families, false, rocksdb_config, metrics)
    }

    pub fn open_with_cfs(
        root_path: impl AsRef<Path>,
        column_families: Vec<ColumnFamilyName>,
        readonly: bool,
        rocksdb_config: RocksdbConfig,
        metrics: Option<StoreMetrics>,
    ) -> Result<Self> {
        let mut rocksdb_opts = Self::gen_rocksdb_options(&rocksdb_config);
        check_open_fds_limit(rocksdb_config.max_open_files as u64 + RES_FDS)?;

        let path = root_path.as_ref();
        let path_str = root_path.as_ref().to_str().unwrap();

        let cfs_set: HashSet<_> = column_families.iter().collect();
        {
            ensure!(
                cfs_set.len() == column_families.len(),
                "duplicate column family name found.",
            );
        }
        if Self::db_exists(path) {
            let cf_vec = Self::list_cf(path_str)?;
            let mut db_cfs_set: HashSet<_> = cf_vec.iter().collect();
            db_cfs_set.remove(&DEFAULT_PREFIX_NAME.to_string());
            ensure!(
                db_cfs_set.len() <= cfs_set.len(),
                RawStoreError::StoreCheckError(format_err!(
                    "ColumnFamily in db ({:?}) not same as ColumnFamily in code {:?}.",
                    column_families,
                    cf_vec
                ))
            );
            let mut remove_cf_vec = Vec::new();
            db_cfs_set.iter().for_each(|k| {
                if !cfs_set.contains(&k.as_str()) {
                    remove_cf_vec.push(<&String>::clone(k));
                }
            });
            ensure!(
                remove_cf_vec.is_empty(),
                RawStoreError::StoreCheckError(format_err!(
                    "can not remove ColumnFamily, ColumnFamily in db ({:?}) not in code {:?}.",
                    remove_cf_vec,
                    cf_vec
                ))
            );
        }

        let db = if readonly {
            Self::open_readonly(rocksdb_opts, path_str, column_families.clone())?
        } else {
            rocksdb_opts.create_if_missing(true);
            rocksdb_opts.create_missing_column_families(true);
            Self::open_inner(rocksdb_opts, path_str, column_families.clone())?
        };

        Ok(RocksDB {
            db: Arc::new(db),
            _cfs: column_families,
            metrics,
        })
    }

    fn open_inner(opts: DBOptions, path: &str, cfs: Vec<ColumnFamilyName>) -> Result<DB> {
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.compression(DBCompressionType::Lz4);
        let cfs_opts = vec![cf_opts; cfs.len()];
        let cfs_descriptor: Vec<_> = cfs
            .iter()
            .zip(cfs_opts)
            .map(|(&name, options)| ColumnFamilyDescriptor::new(name, options))
            .collect();
        DB::open_cf(opts, path, cfs_descriptor).map_err(r2e)
    }

    fn open_readonly(
        db_opts: DBOptions,
        path: &str,
        column_families: Vec<ColumnFamilyName>,
    ) -> Result<DB> {
        let error_if_log_file_exists = false;
        DB::open_cf_for_read_only(db_opts, path, column_families, error_if_log_file_exists)
            .map_err(r2e)
    }

    pub fn list_cf(path: &str) -> Result<Vec<String>, Error> {
        DB::list_column_families(&DBOptions::default(), path).map_err(r2e)
    }

    fn db_exists(path: &Path) -> bool {
        let rocksdb_current_file = path.join("CURRENT");
        rocksdb_current_file.is_file()
    }

    fn get_cf_handle(&self, cf_name: &str) -> &CFHandle {
        self.db.cf_handle(cf_name).unwrap_or_else(|| {
            panic!(
                "DB::cf_handle not found for column family name: {}",
                cf_name
            )
        })
    }

    fn default_write_options() -> WriteOptions {
        let mut opts = WriteOptions::new();
        opts.set_sync(false);
        opts
    }

    fn sync_write_options() -> WriteOptions {
        let mut opts = WriteOptions::new();
        opts.set_sync(true);
        opts
    }

    fn gen_rocksdb_options(config: &RocksdbConfig) -> DBOptions {
        let mut opts = DBOptions::default();
        opts.set_max_open_files(config.max_open_files);
        opts.set_max_total_wal_size(config.max_total_wal_size);
        opts.set_bytes_per_sync(config.bytes_per_sync);
        opts.set_writable_file_max_buffer_size(512 * 1024 * 1024);
        opts.set_use_fsync(false);
        opts.set_table_cache_num_shard_bits(6);
        opts.set_wal_bytes_per_sync(8 * 1024 * 1024);
        opts.set_max_background_jobs(5);
        opts
    }

    fn iter_with_direction<K, V>(
        &self,
        prefix_name: &str,
        direction: ScanDirection,
    ) -> Result<SchemaIterator<K, V>>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let cf_handle = self.get_cf_handle(prefix_name);
        Ok(SchemaIterator::new(
            DBIterator::new_cf(self.db.clone(), cf_handle, ReadOptions::default()),
            direction,
        ))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema.
    pub fn iter<K, V>(&self, prefix_name: &str) -> Result<SchemaIterator<K, V>>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        self.iter_with_direction(prefix_name, ScanDirection::Forward)
    }

    /// Returns a backward [`SchemaIterator`] on a certain schema.
    pub fn rev_iter<K, V>(&self, prefix_name: &str) -> Result<SchemaIterator<K, V>>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        self.iter_with_direction(prefix_name, ScanDirection::Backward)
    }

    // This function updates the 'store_item_bytes' metric
    fn metric_update_store_item_bytes(&self, prefix_name: &str, key: &[u8], value: &[u8]) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics
                .store_item_bytes
                .with_label_values(&[prefix_name])
                .observe((key.len() + value.len()) as f64);
        }
    }
}

pub enum ScanDirection {
    Forward,
    Backward,
}

// FIXME: Would prefer using &DB instead of Arc<DB>.  As elsewhere in
// this crate, it would require generic associated types.
pub struct SchemaIterator<K, V> {
    iter: DBIterator<Arc<DB>>,
    direction: ScanDirection,
    phantom_k: PhantomData<K>,
    phantom_v: PhantomData<V>,
}

impl<K, V> SchemaIterator<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn new(iter: DBIterator<Arc<DB>>, direction: ScanDirection) -> Self {
        SchemaIterator {
            iter,
            direction,
            phantom_k: PhantomData,
            phantom_v: PhantomData,
        }
    }

    /// Seeks to the first key whose binary representation is equal to or greater than that of the
    /// `seek_key`.
    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.iter.seek(rocksdb::SeekKey::Key(key)).map_err(r2e)
    }
    /// Seeks to the last key whose binary representation is less than or equal to that of the
    /// `seek_key`.
    pub fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.iter
            .seek_for_prev(rocksdb::SeekKey::Key(key))
            .map_err(r2e)
    }

    /// Seeks to the first key.
    pub fn seek_to_first(&mut self) -> Result<bool> {
        self.iter.seek(rocksdb::SeekKey::Start).map_err(r2e)
    }

    /// Seeks to the last key.
    pub fn seek_to_last(&mut self) -> Result<bool> {
        self.iter.seek(rocksdb::SeekKey::End).map_err(r2e)
    }

    fn next_impl(&mut self) -> Result<Option<(K, V)>> {
        if !self.valid()? {
            return Err(r2e("iterator invalid"));
        }

        let raw_key = self.iter.key();
        let raw_value = self.iter.value();
        let key = from_bytes::<K>(raw_key)?;
        let value = from_bytes::<V>(raw_value)?;
        match self.direction {
            ScanDirection::Forward => {
                let _ = self.iter.next().map_err(r2e)?;
            }
            ScanDirection::Backward => {
                let _ = self.iter.prev().map_err(r2e)?;
            }
        }

        Ok(Some((key, value)))
    }

    fn valid(&self) -> Result<bool> {
        self.iter.valid().map_err(r2e)
    }
}

impl<K, V> Iterator for SchemaIterator<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl().transpose()
    }
}

impl DBStore for RocksDB {
    fn get(&self, prefix_name: &str, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        record_metrics("db", prefix_name, "get", self.metrics.as_ref()).call(|| {
            let cf_handle = self.get_cf_handle(prefix_name);
            let v = self.db.get_cf(cf_handle, key.as_slice()).map_err(r2e)?;
            Ok(v.map(|db_vector| db_vector.to_vec()))
        })
    }

    fn put(&self, prefix_name: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.metric_update_store_item_bytes(prefix_name, &key, &value);

        record_metrics("db", prefix_name, "put", self.metrics.as_ref()).call(|| {
            let cf_handle = self.get_cf_handle(prefix_name);
            self.db
                .put_cf_opt(cf_handle, &key, &value, &Self::default_write_options())
                .map_err(r2e)
        })
    }

    fn contains_key(&self, prefix_name: &str, key: Vec<u8>) -> Result<bool> {
        record_metrics("db", prefix_name, "contains_key", self.metrics.as_ref()).call(|| match self
            .get(prefix_name, key)
        {
            Ok(Some(_)) => Ok(true),
            _ => Ok(false),
        })
    }

    fn remove(&self, prefix_name: &str, key: Vec<u8>) -> Result<()> {
        record_metrics("db", prefix_name, "remove", self.metrics.as_ref()).call(|| {
            let cf_handle = self.get_cf_handle(prefix_name);
            self.db.delete_cf(cf_handle, &key).map_err(r2e)
        })
    }

    /// Writes a group of records wrapped in a WriteBatch.
    fn write_batch(&self, prefix_name: &str, batch: WriteBatch) -> Result<()> {
        record_metrics("db", prefix_name, "write_batch", self.metrics.as_ref()).call(|| {
            let db_batch = DBWriteBatch::default();
            let cf_handle = self.get_cf_handle(prefix_name);
            for (key, write_op) in &batch.rows {
                match write_op {
                    WriteOp::Value(value) => db_batch.put_cf(cf_handle, key, value).map_err(r2e)?,
                    WriteOp::Deletion => db_batch.delete_cf(cf_handle, key).map_err(r2e)?,
                };
            }
            self.db
                .write_opt(&db_batch, &Self::default_write_options())
                .map_err(r2e)
        })
    }

    fn get_len(&self) -> Result<u64> {
        unimplemented!()
    }

    fn keys(&self) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn put_sync(&self, prefix_name: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.metric_update_store_item_bytes(prefix_name, &key, &value);

        record_metrics("db", prefix_name, "put_sync", self.metrics.as_ref()).call(|| {
            let cf_handle = self.get_cf_handle(prefix_name);
            self.db
                .put_cf_opt(cf_handle, &key, &value, &Self::sync_write_options())
                .map_err(r2e)
        })
    }

    fn write_batch_sync(&self, prefix_name: &str, batch: WriteBatch) -> Result<()> {
        record_metrics("db", prefix_name, "write_batch_sync", self.metrics.as_ref()).call(|| {
            let db_batch = DBWriteBatch::default();
            let cf_handle = self.get_cf_handle(prefix_name);
            for (key, write_op) in &batch.rows {
                match write_op {
                    WriteOp::Value(value) => db_batch.put_cf(cf_handle, key, value).map_err(r2e)?,
                    WriteOp::Deletion => db_batch.delete_cf(cf_handle, key).map_err(r2e)?,
                };
            }
            self.db
                .write_opt(&db_batch, &Self::sync_write_options())
                .map_err(r2e)
        })
    }

    fn multi_get(&self, prefix_name: &str, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>> {
        record_metrics("db", prefix_name, "multi_get", self.metrics.as_ref()).call(|| {
            let cf_handle = self.get_cf_handle(prefix_name);

            let mut results = Vec::new();
            for key in keys {
                let value_opt = self.db.get_cf(cf_handle, &key).map_err(r2e)?;
                let value_opt = value_opt.map(|db_vector| db_vector.to_vec());
                results.push(value_opt);
            }

            Ok(results)
        })
    }
}
