# Tapedrive

--------------------------------------

> [!Important]
> Tapedrive is very much still in development, it is **not deployed** and is **currently private**.
> 
> This repository contains an early proof-of-concept of **TapeReplay**, the [official version](https://x.com/zelimir__/status/1975304835064078660) is in the works but invite only at the moment.
> 
> [Sign up](https://tapedrive.io/#sign-up) to receive updates on network milestones and product releases. Join us on [Discord](https://discord.gg/dVa9TWA45X) to engage with our devs.
--------------------------------------


[![crates.io](https://img.shields.io/crates/v/tapedrive-cli.svg?style=flat)](https://crates.io/crates/tapedrive-cli)

**Decentralized object storage** built to retrieve any type of data. Tapedrive turns unused storage into a global, verifiable cloud.

![image](https://github.com/user-attachments/assets/bf674eb1-512d-47e7-a9c5-e0d0e44c6edb)


--------------------------------------

Cloud platforms succeeded not by being cheap, but by being convenient to start and operate: no hardware to buy, minimal friction, and predictable workflows. By contrast, most decentralized storage systems impose conceptual overhead, operational burden, or unreliable performance that prevents mainstream use.

Tapedrive addresses this gap. Our objective is to deliver a decentralized storage network that "just works", with a product experience closer to AWS S3 while retaining verifiable integrity and open participation. We focus on simplicity, convenience, and scalability.

## Design Principles

Our north star is infinite scale via simplicity:

- Simplicity: choose the simplest viable design for both users and operators.
- Convenience: reduce effort and time-to-first-byte; eliminate sharp edges.
- Scale: design for horizontal growth without coordination bottlenecks.


## System Architecture

Tapedrive is the storage layer of the Tape Network, designed to minimize operator burden while offering a cloud-like developer experience.

<img width="1191" height="497" alt="image" src="https://github.com/user-attachments/assets/8bc74066-3ee9-48da-b8e9-b99c9ab68d88" />

**Control plane** Our onchain programs maintain node comitteees, stake weight, track commitments, enforce consensus. The design operates within Solana’s runtime despite tighter compute/state limits and the absence of BLS12-381.

**Data plane** Objects are erasure coded; fragments are distributed across nodes to tolerate failures while enabling efficient reconstruction. Read paths retrieve the minimal set of fragments with caching for hot objects.

**TapeReplay** Our small-object pipeline, TapeReplay, provides efficient metadata handling, addressing a common weakness in decentralized storage systems.

**Consensus** We use a similar scheme to Alpenglow with a custom aggregate signature scheme over [curve-254](https://hackmd.io/@jpw/bn254) to compresses many node attestations into compact on-chain proofs, reducing control-plane transaction volume. We may switch over to BLS12-381 if `min_pk` becomes possible within the SVM at some future point.

## Want to run a node?

[Sign up](https://tapedrive.io/#sign-up) for early access, Tapedrive is currently private.

