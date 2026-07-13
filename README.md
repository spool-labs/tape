# Tapedrive

Tapedrive runs its own network of storage nodes, much like a blockchain runs its
own validators. Instead of validating transactions, the nodes store files, and they keep
proving your data is still there. 

To you, it just looks like S3: same tools, new endpoint.

> [!NOTE]
> Tapedrive is in early access and invite-only. 
> 
> [Sign up](https://tape.network/#sign-up) for access, or join the [Discord](https://discord.gg/dVa9TWA45X) to follow development,
> and read the [docs](https://docs.tape.network) for the full picture.


## What you get

Your data is distributed to many nodes on the network. Every large write is [erasure-coded](https://docs.tape.network/protocol/architecture/slicing), onto a spool group. The network runs many groups, with no cap on the number of groups or nodes.

Storage overhead is about 2.8x the stored size, far less than other storage networks. Repair is **bandwidth-optimal**: a lost piece is rebuilt from small fragments read across many nodes, not from whole copies.

The [S3-gateway](https://docs.tape.network/tools/s3-gateway) means most existing tooling works after changing an endpoint and a credential.

When you want signed writes and verified reads straight from your own code, the [CLI](https://docs.tape.network/tools/cli) and [SDKs](https://docs.tape.network/sdks/quickstart) speak the network natively.


## Try it

![Terminal recording of tape create, tape write, and tape read](tools/tape/demos/write.gif)


> Install the [CLI](https://docs.tape.network/tools/cli). Prefer code? The [SDK quickstart](https://docs.tape.network/sdks/quickstart) walks the same flow in six languages.
> 
> Already on S3? Point your existing tooling at the [S3-gateway](https://docs.tape.network/tools/s3-gateway) and keep your workflow.


## Run a node

Storage nodes stake TAPE, hold slices of the network's data, and earn for proving they
still have them, on hardware you own or rent. The
[node setup guide](https://docs.tape.network/protocol/node-setup) takes you from a bare
Linux machine to a registered, earning node.


## Learn more

- [Docs](https://docs.tape.network): from the
  [quickstart](https://docs.tape.network/protocol/quickstart) to the
  [white paper](https://docs.tape.network/protocol/architecture/white-paper).
- [When to use Tapedrive](https://docs.tape.network/protocol#when-to-use-tapedrive), and
  when not to.
- [Explorer](https://explorer.tape.network): watch the network live.
- [Discord](https://discord.gg/dVa9TWA45X) and [X](https://x.com/tapedrive_io).

Looking for the earlier Proof-of-Work version? It lives on the
[`PoW`](https://github.com/spool-labs/tape/tree/pow) branch.
