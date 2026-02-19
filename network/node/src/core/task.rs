#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskCategory {
    SolanaTx,
    PeerHttp,
    CpuHeavy,
    Internal,
}
