use rocksdb::{
    ColumnFamilyDescriptor, DBCompressionType, Options,
    PlainTableFactoryOptions, SliceTransform, DB, CompactOptions, BlockBasedOptions,
};
use tempdir::TempDir;
use rand::{Rng, RngCore};
use std::time::Instant;
use std::path::Path;
use std::fs;

const NUM_HASHES: usize = 10_000;
const NUM_VALUES_PER_HASH: usize = 1_000;
const SEGMENT_SIZE: usize = 128; // bytes

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnFamily {
    WithHash,
    WithoutHash
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableFormat {
    PlainTable,
    BlockBased
}

impl ColumnFamily {
    pub fn as_str(&self) -> &'static str {
        match self {
            ColumnFamily::WithHash => "with_hash",
            ColumnFamily::WithoutHash => "without_hash",
        }
    }
}

fn base_options() -> Options {
    let mut opts = Options::default();
    opts.set_compression_type(DBCompressionType::None);
    opts
}

fn plain_table_options(user_key_length: u32) -> PlainTableFactoryOptions {
    PlainTableFactoryOptions {
        user_key_length,
        bloom_bits_per_key: 10,
        hash_table_ratio: 0.75,
        index_sparseness: 16,
        huge_page_tlb_size: 0,
        encoding_type: rocksdb::KeyEncodingType::Prefix,
        full_scan_mode: false,
        store_index_in_file: false,
    }
}

fn block_based_options() -> BlockBasedOptions {
    let mut opts = BlockBasedOptions::default();
    opts.set_block_size(64 * 1024);
    opts.set_bloom_filter(10.0, false);
    opts.set_cache_index_and_filter_blocks(true);
    opts
}

pub fn create_cf_descriptor(cf: ColumnFamily, format: TableFormat) -> ColumnFamilyDescriptor {
    let (prefix_len, plain_table, block_based) = match cf {
        ColumnFamily::WithHash => (32, format == TableFormat::PlainTable, format == TableFormat::BlockBased),
        ColumnFamily::WithoutHash => (8, format == TableFormat::PlainTable, format == TableFormat::BlockBased),
    };
    let mut opts = base_options();
    if prefix_len > 0 {
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len));
    }
    if plain_table {
        opts.set_plain_table_factory(&plain_table_options(if cf == ColumnFamily::WithHash { 40 } else { 8 }));
    }
    if block_based {
        opts.set_block_based_table_factory(&block_based_options());
    }
    ColumnFamilyDescriptor::new(cf.as_str(), opts)
}

pub struct TestStore {
    pub db: DB,
}

impl TestStore {
    pub fn new<P: AsRef<Path>>(path: P, cf: ColumnFamily, format: TableFormat) -> Result<Self, rocksdb::Error> {
        let path = path.as_ref();
        let cfs = vec![create_cf_descriptor(cf, format)];
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_size(64 * 1024 * 1024); // 64 MB
        db_opts.set_max_write_buffer_number(3);
        db_opts.increase_parallelism(num_cpus::get() as i32);
        let db = DB::open_cf_descriptors(&db_opts, path, cfs)?;
        Ok(Self { db })
    }
}

fn get_db_file_size(path: &Path) -> u64 {
    let mut total_size = 0;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.filter_map(Result::ok) {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    total_size += metadata.len();
                }
            }
        }
    }
    total_size
}

fn main() {
    let configs = [
        (ColumnFamily::WithHash, TableFormat::PlainTable, "with_hash_plain"),
        (ColumnFamily::WithHash, TableFormat::BlockBased, "with_hash_block"),
        (ColumnFamily::WithoutHash, TableFormat::PlainTable, "without_hash_plain"),
        (ColumnFamily::WithoutHash, TableFormat::BlockBased, "without_hash_block"),
    ];

    for (cf, format, id) in configs {
        let temp_dir = TempDir::new(&format!("rocksdb_bench_{}", id)).expect("Failed to create temp dir");
        let store = TestStore::new(temp_dir.path(), cf, format).expect("Failed to create TestStore");

        let cf_handle = store.db.cf_handle(cf.as_str()).expect("Failed to get CF handle");

        let start = Instant::now();

        let mut rng = rand::thread_rng();
        if cf == ColumnFamily::WithHash {
            for _ in 0..NUM_HASHES {
                let hash: [u8; 32] = rng.gen();
                for _ in 0..NUM_VALUES_PER_HASH {
                    let suffix: [u8; 8] = rng.gen();
                    let mut value = [0u8; SEGMENT_SIZE];
                    rng.fill_bytes(&mut value); 
                    let mut key = Vec::with_capacity(40);
                    key.extend_from_slice(&hash);
                    key.extend_from_slice(&suffix);
                    store.db.put_cf(&cf_handle, &key, value).expect("Put failed");
                }
            }
        } else {
            for _ in 0..(NUM_HASHES * NUM_VALUES_PER_HASH) {
                let key: [u8; 8] = rng.gen(); 
                let mut value = [0u8; SEGMENT_SIZE];
                rng.fill_bytes(&mut value); 
                store.db.put_cf(&cf_handle, key, value).expect("Put failed");
            }
        }

        store.db.flush_cf(&cf_handle).expect("Flush failed");
        let mut compact_opts = CompactOptions::default();
        store.db.compact_range_cf_opt(&cf_handle, None::<&[u8]>, None::<&[u8]>, &mut compact_opts);

        let size = get_db_file_size(temp_dir.path());
        let elapsed = start.elapsed();
        println!("{}: Size = {} bytes, Time = {:?}", id, size, elapsed);
    }
}
