use tokio::sync::mpsc;
use solana_sdk::pubkey::Pubkey;

pub const QUEUE_CAP: usize = 10_000;

#[derive(Debug)]
pub struct SegmentJob {
    pub tape: Pubkey,
    pub seg_no: u64,
    pub data: Vec<u8>,
}

pub type Tx = mpsc::Sender<SegmentJob>;
pub type Rx = mpsc::Receiver<SegmentJob>;

pub fn channel() -> (Tx, Rx) {
    mpsc::channel::<SegmentJob>(QUEUE_CAP)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel() {
        let (tx, rx) = channel();
        assert_eq!(tx.capacity(), QUEUE_CAP);
        assert_eq!(rx.capacity(), QUEUE_CAP);
    }

    #[test]
    fn test_segment_job() {
        let job = SegmentJob {
            tape: Pubkey::new_unique(),
            seg_no: 1,
            data: vec![1, 2, 3],
        };
        assert_eq!(job.seg_no, 1);
        assert!(!job.data.is_empty());
    }
}
