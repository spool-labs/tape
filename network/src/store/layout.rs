use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, DBCompressionType, Options, PlainTableFactoryOptions,
    SliceTransform,
};

#[derive(Clone, Copy, Debug)]
pub enum ColumnFamily {
    TapeByNumber,
    TapeByAddress,
    TapeSegments,
    Sectors,
    MerkleHashes,
    Health,
}

impl ColumnFamily {
    pub fn as_str(&self) -> &'static str {
        match self {
            ColumnFamily::TapeByNumber => "tape_by_number",
            ColumnFamily::TapeByAddress => "tape_by_address",
            ColumnFamily::TapeSegments => "tape_segments",
            ColumnFamily::Sectors => "sectors",
            ColumnFamily::MerkleHashes => "merkle_hashes",
            ColumnFamily::Health => "health",
        }
    }
}

pub fn create_cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
    let mut cf_tape_by_number_opts = Options::default();
    cf_tape_by_number_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
    cf_tape_by_number_opts.set_plain_table_factory(&PlainTableFactoryOptions {
        user_key_length: 8,
        bloom_bits_per_key: 10,
        hash_table_ratio: 0.75,
        index_sparseness: 16,
        huge_page_tlb_size: 0,
        encoding_type: rocksdb::KeyEncodingType::Prefix,
        full_scan_mode: false,
        store_index_in_file: false,
    });
    cf_tape_by_number_opts.set_compression_type(DBCompressionType::None);

    let mut cf_tape_by_address_opts = Options::default();
    cf_tape_by_address_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
    cf_tape_by_address_opts.set_plain_table_factory(&PlainTableFactoryOptions {
        user_key_length: 32,
        bloom_bits_per_key: 10,
        hash_table_ratio: 0.75,
        index_sparseness: 16,
        huge_page_tlb_size: 0,
        encoding_type: rocksdb::KeyEncodingType::Prefix,
        full_scan_mode: false,
        store_index_in_file: false,
    });
    cf_tape_by_address_opts.set_compression_type(DBCompressionType::None);

    let mut cf_tape_segments_opts = Options::default();
    cf_tape_segments_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
    cf_tape_segments_opts.set_compression_type(DBCompressionType::None);

    let mut cf_sectors_opts = Options::default();
    cf_sectors_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
    let mut bbt_sectors = BlockBasedOptions::default();
    bbt_sectors.set_block_size(16 * 1024);
    bbt_sectors.set_bloom_filter(10.0, false);
    bbt_sectors.set_cache_index_and_filter_blocks(true);
    cf_sectors_opts.set_block_based_table_factory(&bbt_sectors);
    cf_sectors_opts.set_level_compaction_dynamic_level_bytes(true);
    cf_sectors_opts.set_compression_type(DBCompressionType::None);

    let mut cf_merkle_hashes_opts = Options::default();
    cf_merkle_hashes_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
    let mut bbt_merkle = BlockBasedOptions::default();
    bbt_merkle.set_block_size(16 * 1024);
    bbt_merkle.set_bloom_filter(10.0, false);
    bbt_merkle.set_cache_index_and_filter_blocks(true);
    cf_merkle_hashes_opts.set_block_based_table_factory(&bbt_merkle);
    cf_merkle_hashes_opts.set_level_compaction_dynamic_level_bytes(true);
    cf_merkle_hashes_opts.set_compression_type(DBCompressionType::None);

    let mut cf_health_opts = Options::default();
    cf_health_opts.set_compression_type(DBCompressionType::None);

    let cf_tape_by_number = ColumnFamilyDescriptor::new(ColumnFamily::TapeByNumber.as_str(), cf_tape_by_number_opts);
    let cf_tape_by_address = ColumnFamilyDescriptor::new(ColumnFamily::TapeByAddress.as_str(), cf_tape_by_address_opts);
    let cf_tape_segments = ColumnFamilyDescriptor::new(ColumnFamily::TapeSegments.as_str(), cf_tape_segments_opts);
    let cf_sectors = ColumnFamilyDescriptor::new(ColumnFamily::Sectors.as_str(), cf_sectors_opts);
    let cf_merkle_hashes = ColumnFamilyDescriptor::new(ColumnFamily::MerkleHashes.as_str(), cf_merkle_hashes_opts);
    let cf_health = ColumnFamilyDescriptor::new(ColumnFamily::Health.as_str(), cf_health_opts);

    vec![
        cf_tape_by_number,
        cf_tape_by_address,
        cf_tape_segments,
        cf_sectors,
        cf_merkle_hashes,
        cf_health,
    ]
}
