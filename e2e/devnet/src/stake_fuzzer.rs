use rand::Rng;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use tape_api::program::tapedrive::node_pda;
use tape_core::types::coin::TAPE;
use tape_sdk::keys::stake_key::StakeKey;
use tape_sdk::tapedrive::Tapedrive;

enum DelegationPhase {
    Idle,
    Staked { pool: Pubkey },
    Unlocking { pool: Pubkey },
}

struct DelegatedStake {
    key: StakeKey,
    phase: DelegationPhase,
}

pub struct StakeFuzzer {
    delegations: Vec<DelegatedStake>,
    pub tx_succeeded: u64,
    pub tx_failed: u64,
}

impl StakeFuzzer {
    pub fn new() -> Self {
        let delegations = (0..20)
            .map(|_| DelegatedStake {
                key: StakeKey::generate(),
                phase: DelegationPhase::Idle,
            })
            .collect();
        Self {
            delegations,
            tx_succeeded: 0,
            tx_failed: 0,
        }
    }

    pub async fn step_epoch(
        &mut self,
        rpc: &LiteSvmRpc,
        admin: &Keypair,
        node_authorities: &[Pubkey],
    ) {
        if node_authorities.is_empty() {
            return;
        }

        let sdk = Tapedrive::new(rpc.clone(), admin);

        // Advance all node pools (tolerate errors)
        for auth in node_authorities {
            let (pool, _) = node_pda(*auth);
            if let Err(e) = sdk.advance_pool(*auth, pool).await {
                tracing::error!("advance_pool: {e:#}");
            }
        }

        let mut rng = rand::thread_rng();
        let count = rng.gen_range(1..=3usize).min(self.delegations.len());
        let indices: Vec<usize> = rand::seq::index::sample(&mut rng, self.delegations.len(), count)
            .into_vec();

        for idx in indices {
            let auth_idx = rng.gen_range(0..node_authorities.len());
            let node_auth = node_authorities[auth_idx];
            let (pool, _) = node_pda(node_auth);

            let d = &mut self.delegations[idx];
            match &d.phase {
                DelegationPhase::Idle => {
                    let amount_tape = rng.gen_range(10u64..=500);
                    let amount = TAPE(amount_tape * TAPE::SCALE);
                    match sdk.stake_with_pool(&d.key, pool, amount).await {
                        Ok(()) => {
                            tracing::info!("staked {amount_tape} TAPE with pool");
                            d.phase = DelegationPhase::Staked { pool };
                            self.tx_succeeded += 1;
                        }
                        Err(e) => {
                            tracing::error!("stake_with_pool: {e:#}");
                            self.tx_failed += 1;
                        }
                    }
                }
                DelegationPhase::Staked { pool } => {
                    let pool = *pool;
                    match sdk.request_stake_unlock(&d.key, pool).await {
                        Ok(()) => {
                            tracing::info!("requested stake unlock");
                            d.phase = DelegationPhase::Unlocking { pool };
                            self.tx_succeeded += 1;
                        }
                        Err(e) => {
                            tracing::error!("request_stake_unlock: {e:#}");
                            self.tx_failed += 1;
                        }
                    }
                }
                DelegationPhase::Unlocking { pool } => {
                    let pool = *pool;
                    match sdk.unstake_from_pool(&d.key, pool).await {
                        Ok(()) => {
                            tracing::info!("unstaked from pool");
                            d.phase = DelegationPhase::Idle;
                            d.key = StakeKey::generate();
                            self.tx_succeeded += 1;
                        }
                        Err(e) => {
                            tracing::error!("unstake_from_pool: {e:#}");
                            self.tx_failed += 1;
                        }
                    }
                }
            }
        }
    }
}
