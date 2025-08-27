use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, DBCompressionType, Options,
    PlainTableFactoryOptions, SliceTransform,
};

#[derive(Clone, Copy, Debug)]
pub enum ColumnFamily {
    TapeByNumber,
    TapeByAddress,
    Sectors,
    MerkleHashes,
    Health,
}

impl ColumnFamily {
    pub fn as_str(&self) -> &'static str {
        match self {
            ColumnFamily::TapeByNumber => "tape_by_number",
            ColumnFamily::TapeByAddress => "tape_by_address",
            ColumnFamily::Sectors => "sectors",
            ColumnFamily::MerkleHashes => "merkle_hashes",
            ColumnFamily::Health => "health",
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
    opts.set_block_size(16 * 1024);
    opts.set_bloom_filter(10.0, false);
    opts.set_cache_index_and_filter_blocks(true);
    opts
}

pub fn create_cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
    let column_families = [
        (ColumnFamily::TapeByNumber, 8, Some(plain_table_options(8)), None),
        (ColumnFamily::TapeByAddress, 32, Some(plain_table_options(32)), None),
        (ColumnFamily::Sectors, 32, None, Some(block_based_options())),
        (ColumnFamily::MerkleHashes, 32, None, Some(block_based_options())),
        (ColumnFamily::Health, 0, None, None),
    ];

    column_families
        .into_iter()
        .map(|(cf, prefix_len, plain_table, block_based)| {
            let mut opts = base_options();
            if prefix_len > 0 {
                opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len));
            }
            if let Some(plain_table_opts) = plain_table {
                opts.set_plain_table_factory(&plain_table_opts);
            }
            if let Some(block_based_opts) = block_based {
                opts.set_block_based_table_factory(&block_based_opts);
                opts.set_level_compaction_dynamic_level_bytes(true);
            }
            ColumnFamilyDescriptor::new(cf.as_str(), opts)
        })
        .collect()
}
