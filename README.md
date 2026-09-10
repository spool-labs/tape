# Tapedrive

Tapedrive is fast object storage you can run on your computer, deploy to hardware you
control, or use through Tapenet, the open network of Tapedrive nodes.

Write a file, get an address, and share it. If you know Git, HTTP, or S3, you already
know how to use Tapedrive.

> [!NOTE]
> Tapedrive is in early access. Read the [docs](https://docs.tape.network) or join the
> [Discord](https://discord.gg/dVa9TWA45X) to follow development and get help with
> devnet.


## What you get

Tapedrive uses [erasure coding](https://docs.tape.network/protocol/architecture/slicing),
verification, and bandwidth-optimal repair to keep data available when machines fail.
Large writes move directly between the client and storage nodes.

Use the [CLI](https://docs.tape.network/tools/cli), clone and push Git repositories with
`tape://` addresses, point existing tools at the
[S3 gateway](https://docs.tape.network/tools/s3-gateway), or use an
[SDK](https://docs.tape.network/sdks/quickstart) from your own code.


## How it works

Solana is the control plane. It tracks participating nodes, where erasure-coded pieces
are stored, and the state everyone agrees on. Tapedrive nodes are the data plane. They
store and serve the data itself.

This gives Tapedrive a fast, fully replicated write-ahead log without putting Solana in
the data path. Local Tapedrive uses the same design with a light, fast Solana control
plane.


## Try it

The Tapedrive installer is coming soon:

```bash
curl -fsSL https://tape.network/install.sh | sh
```

Clone any Tapedrive repository:

```bash
git clone tape://<address>
```

<img src="https://github.com/user-attachments/assets/d223349d-0b72-42c3-9332-9e35e1383440" />

The [quickstart](https://docs.tape.network/protocol/quickstart) shows how to choose local
storage or Tapenet and write your first object.


## Use Tapenet

Tapenet is an open network of independently run Tapedrive nodes. Use it when you do not
want to run the storage yourself, or join with hardware you control.

If you want to use devnet or run a node while we are pre-mainnet, join us on
[Discord](https://discord.gg/dVa9TWA45X) and we will help you get set up. The
[node setup guide](https://docs.tape.network/protocol/node-setup) covers the operator
path.


## Learn more

- Read the [docs](https://docs.tape.network), starting with the
  [quickstart](https://docs.tape.network/protocol/quickstart).
- Watch Tapenet in the [explorer](https://explorer.tape.network).
- Follow [X](https://x.com/tapedrive_io) or join the
  [Discord](https://discord.gg/dVa9TWA45X).

Looking for the earlier Proof-of-Work version? It lives on the
[`PoW`](https://github.com/spool-labs/tape/tree/pow) branch.
