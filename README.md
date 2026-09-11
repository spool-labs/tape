# Tapedrive

Tapedrive is fast object storage you can run on your computer, deploy to hardware you
control, or use through Tapenet, the open network of Tapedrive nodes.

> [!NOTE]
> Tapedrive is in early access. Read the [docs](https://docs.tape.network) or join the
> [Discord](https://discord.gg/dVa9TWA45X) to follow development and get help with
> devnet.


## How it works

We use Solana as the control plane. It tracks participating nodes, where erasure-coded 
pieces are stored, and the state everyone agrees on. Tapedrive nodes are the data plane
and store the data itself.

This gives Tapedrive a fast, fully replicated log without putting Solana in the data 
path. Local Tapedrive uses the same design with a light, fast Solana control
plane.


## No Hardware?

Tapenet is an open network of independently run Tapedrive nodes. Use it when you do not
want to run the storage yourself, or join with hardware you control.

> [!NOTE] 
> If you want to use devnet or run a node while we are pre-mainnet, join us on [Discord](https://discord.gg/dVa9TWA45X) and we will help you get set up. The [node setup guide](https://docs.tape.network/protocol/node-setup) covers the operator path.

## Try it

> Install the [CLI](https://docs.tape.network/tools/cli). Prefer code? The [SDK quickstart](https://docs.tape.network/sdks/quickstart) walks the same flow in multiple languages.

<img src="https://github.com/user-attachments/assets/d223349d-0b72-42c3-9332-9e35e1383440" />

> Already on S3? Point your existing tooling at the [S3-gateway](https://docs.tape.network/tools/s3-gateway) and keep your workflow.

## Learn more

- Read the [docs](https://docs.tape.network), starting with the
  [quickstart](https://docs.tape.network/protocol/quickstart).
- Watch Tapenet in the [explorer](https://explorer.tape.network).
- Follow [X](https://x.com/tapedrive_io) or join the
  [Discord](https://discord.gg/dVa9TWA45X).

Looking for the earlier Proof-of-Work version? It lives on the
[`PoW`](https://github.com/spool-labs/tape/tree/pow) branch.
