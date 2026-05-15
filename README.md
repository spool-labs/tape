# Tapedrive

  --------------------------------------

  > [!Warning]
  > Tapedrive is under active development and **not yet deployed**.
  >
  > This is the stable **proof-of-stake** rewrite. `Stable` means it compiles
  > and has hit a structural milestone — not that it's usable yet. To
  > follow along with day-to-day progress, check the [`latest`](https://github.com/spool-labs/tape/tree/latest) branch.
  >
  > Looking for the previous proof-of-work version? See the
  > [`pow`](https://github.com/spool-labs/tape/tree/pow) branch.
  >
  > [Sign up](https://tape.network/#sign-up) for early access. Join us on
  > [Discord](https://discord.gg/dVa9TWA45X) to follow development.

  --------------------------------------

  **Storage that scales with agents.** Decentralized object storage,
  Solana-native, designed for the workloads agents actually create.

  ![image](https://github.com/user-attachments/assets/bf674eb1-512d-47e7-a9c5-e0d0e44c6edb)

  --------------------------------------

  Decentralized storage has matured, but most systems miss the bar for
  everyday, object-centric workloads — small files incur metadata that can
  dwarf the payload, large files demand specialized hardware, and
  operational overhead pushes developers back to centralized clouds.
  Tapedrive is built to feel natively S3-like across the full size spectrum,
  with chain-native writes for AI agents and onchain programs that produce
  massive volumes of small context objects (tool outputs, summaries,
  checkpoints).

  ## Design Principles

  Our north star is infinite scale via simplicity:

  - **Simplicity:** choose the simplest viable design for both users and operators.
  - **Convenience:** reduce effort and time-to-first-byte; eliminate sharp edges.
  - **Scale:** design for horizontal growth without coordination bottlenecks.

  ## System Architecture

  Tapedrive is the storage layer of the Tape Network, designed to minimize
  operator burden while offering a cloud-like developer experience. The system
  is built around three integrated mechanisms.

  <img width="1191" height="497" alt="image" src="https://github.com/user-attachments/assets/8bc74066-3ee9-48da-b8e9-b99c9ab68d88" />

  **Tape Replay** — An append-only log that lets agents and programs write
  storage directly from Solana transactions. No separate upload step, no SDK
  handshake. Eliminates per-object metadata bloat and makes storage composable
  with on-chain logic.

  **Tape Slicer** — Adaptive erasure coding that adjusts stripe parameters to
  object size. Small files get lightweight encoding; large files get full
  redundancy. Near-optimal storage overhead and bandwidth-efficient repair
  across the full size spectrum.

  **Tape Spooler** — A deterministic allocation protocol that distributes
  storage assignments across epochs while preserving existing assignments
  wherever possible. When nodes join or leave, data rebalances with minimal
  disruption.

  Tapedrive uses Solana as its control plane — staking, committee management,
  track registration, data commitments, and payments all live on-chain. The
  storage network focuses purely on serving and repairing data.

  ## Want to run a node?

  [Sign up](https://tape.network/#sign-up) for early access. Tapedrive is
  currently invite-only.
