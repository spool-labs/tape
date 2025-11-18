# TAPEDRIVE
[![crates.io](https://img.shields.io/crates/v/tapedrive-cli.svg?style=flat)](https://crates.io/crates/tapedrive-cli)

**Decentralized object storage** on Solana. Tapedrive turns unused storage into a global, verifiable cloud—lower cost, higher integrity, with no lock-in.

![image](https://github.com/user-attachments/assets/bf674eb1-512d-47e7-a9c5-e0d0e44c6edb)

--------------------------------------

> [!Important]
> Tapedrive is very much still in development, it is **not deployed** or public yet. 
> 
> This repository contains a proof-of-concept of **TapeReplay**, the [official version](https://x.com/zelimir__/status/1975304835064078660) is in the works but invite only at the moment.
> 
> [Sign up](https://tapedrive.io/#sign-up) to receive updates on network milestones and product releases. Join us on [Discord](https://discord.gg/dVa9TWA45X) to engage with our devs.

--------------------------------------

## Quick Start

Tapedrive makes it easy to read and write data on Solana. Follow the install instructions below to get started.

#### Write

```
tapedrive write <filepath>
```

```
tapedrive write -m "hello, world"
```

```
tapedrive write -r https://example.com/path/to/a/remote/file
```

#### Read
```
tapedrive read <id>
```

## Install (from source)

If you'd like to try out the latest version, you'll need to build from source. This will require that you have [Rust](https://www.rust-lang.org/tools/install) and [Solana](https://solana.com/docs/intro/installation) tooling installed.

From there, build the tapedrive binary by navigating to `./cli` directory and running the following.

```
cargo install --path .
```

If all goes well, you should now have a `tapedrive` binary.

The latest version may be ahead of the currently deployed solana program, so it is recommended that you run a local validator. This is just one command from the root directory of this repository.

```
make local
```

Keep that running and then run the following tapedrive commands.

```
tapedrive init
tapedrive airdrop 10
```

At this point you're setup.


## How It Works

Whether you're writing a message, a file, or something else, tapedrive compresses the data first, then splits it into chunks that are writen to a tape on the blockchain. Each tape stores the name, number of chunks, byte size, data hash, and the tail of the tape.

When you want to retrieve your data, tapedrive reads the tape sequentially from the tape network or blockchain to reassemble the original data.

----------------------

## Tape://Net

Beyond reading and writing, users can participate in the tape network. There are 3 primary functions, all can run on the same machine. 

<img width="958" alt="image" src="https://github.com/user-attachments/assets/edd81c05-9a23-4d04-9433-602053ed12d5" />

## Want to run a node?

At minimum, each node must run the [archive](#archiving), but from there you can choose to run the [web](#web) service or just [mine](#mining). See the sections below.

> [!Important]
> We have an easy install script for running a **full node**, learn more [here](https://github.com/tapedrive-io/deploy).


## Archiving

If you'd like to either run a public gateway or a miner, you'll need an archiver. You can run one with the following command.

```
tapedrive archive
```

## Mining

You can help secure the tape network by running a miner. You'll be rewarded with the [TAPE](https://explorer.solana.com/address/TAPEv9oFkdiWwq4pMXToy1DnTyki2BW7nLGkKj3iQFu?cluster=devnet) token.

```
tapedrive register <name of your miner>
```

```
tapedrive mine <pubkey from registration>
```

## Web

Miners on the network may run public gateways. You can can run the web service like this.

```
tapedrive web
```

The web service allows users to fetch data using a JSON RPC protocol similar to Solana. The API is accessible at `http://127.0.0.1:3000/api` via HTTP POST requests when running `tapedrive web`.

The following methods currently exist.


### getHealth
Retrieves the last persisted block height and drift.

**Parameters**: None (empty object `{}`)

**Returns**:
```text
{
  "last_processed_slot": <number>,
  "drift": <number>
}
```

**Example**:
```bash
curl -X POST http://127.0.0.1:3000/api \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":5,"method":"getHealth","params":{}}'
```

**Response**:
```text
{
  "jsonrpc": "2.0",
  "result": {
    "last_processed_slot": 123456,
    "drift": 0
  },
  "id": 5
}
```

### getTapeAddress
Retrieves the Solana pubkey (tape address) for a given tape number.

**Parameters**:
```text
{
  "tape_number": <number>
}
```

**Returns**: Base-58-encoded Solana pubkey as a string.

**Example**:
```bash
curl -X POST http://127.0.0.1:3000/api \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getTapeAddress","params":{"tape_number":42}}'
```

**Response**:
```text
{
  "jsonrpc": "2.0",
  "result": "5P6XDRskXsUxyNUk3kA6oU61kWkLxgMX7W5mTvZ3hYRS",
  "id": 1
}
```

### getTapeNumber
Retrieves the numeric tape ID for a given Solana pubkey (tape address).

**Parameters**:
```text
{
  "tape_address": <string>
}
```

**Returns**: Tape number as a number.

**Example**:
```bash
curl -X POST http://127.0.0.1:3000/api \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"getTapeNumber","params":{"tape_address":"5P6XDRskXsUxyNUk3kA6oU61kWkLxgMX7W5mTvZ3hYRS"}}'
```

**Response**:
```text
{
  "jsonrpc": "2.0",
  "result": 42,
  "id": 2
}
```

### getSegment
Fetches a single segment’s data by tape address and segment number.

**Parameters**:
```text
{
  "tape_address": <string>,
  "segment_number": <number>
}
```

**Returns**: Base64-encoded string of the segment’s raw bytes.

**Example**:
```bash
curl -X POST http://127.0.0.1:3000/api \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":3,"method":"getSegment","params":{"tape_address":"5P6XDRskXsUxyNUk3kA6oU61kWkLxgMX7W5mTvZ3hYRS","segment_number":3}}'
```

**Response**:
```text
{
  "jsonrpc": "2.0",
  "result": "SGVsbG8gV29ybGQ=",
  "id": 3
}
```

### getTape
Retrieves all segments and their data for a given tape address.

**Parameters**:
```text
{
  "tape_address": <string>
}
```

**Returns**: Array of objects, each containing:
```text
[
  {
    "segment_number": <number>,
    "data": <string> // Base64-encoded
  }
]
```

**Example**:
```bash
curl -X POST http://127.0.0.1:3000/api \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":4,"method":"getTape","params":{"tape_address":"5P6XDRskXsUxyNUk3kA6oU61kWkLxgMX7W5mTvZ3hYRS"}}'
```

**Response**:
```text
{
  "jsonrpc": "2.0",
  "result": [
    {
      "segment_number": 0,
      "data": "SGVsbG8="
    },
    {
      "segment_number": 1,
      "data": "V29ybGQ="
    }
  ],
  "id": 4
}
```

## Contributing
Fork, PR, or suggest:

Take a look at the `Makefile` if you'd like to build or test the program localy.
