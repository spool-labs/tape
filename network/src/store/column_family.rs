#[derive(Clone, Copy, Debug)]
pub enum ColumnFamily {
    TapeByNumber,
    TapeByAddress,
    TapeStats,
    Sectors,
    MerkleLayers,
    Health,
}

impl ColumnFamily {
    pub fn as_str(&self) -> &'static str {
        match self {
            ColumnFamily::TapeByNumber => "tape_by_number",
            ColumnFamily::TapeByAddress => "tape_by_address",
            ColumnFamily::TapeStats => "tape_stats",
            ColumnFamily::Sectors => "sectors",
            ColumnFamily::MerkleLayers => "merkle_layers",
            ColumnFamily::Health => "health",
        }
    }
}
