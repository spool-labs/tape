// Not Possible On Solana
//
// BLS with min_pk (pubkeys in G1, signatures in G2) isn’t practical on Solana today because the
// runtime lacks the necessary cryptographic syscalls: there is no pairing check (e.g., for
// BLS12-381/BN254), no G2 arithmetic, and no standardized hash-to-curve/cofactor-clearing routines
// for G2. 
//
// Verification in `min_pk` requires hashing the message into G2 and performing a Type-3 pairing
// check e(sig, G1) = e(H(m), pk), operations that can’t be offloaded to the runtime and would
// exceed typical on-chain compute budgets if implemented in pure program code. 
//
// See the Solana Improvement Documents discussion for the required but missing syscalls
// https://github.com/solana-foundation/solana-improvement-documents/discussions/293
