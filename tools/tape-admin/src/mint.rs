use tape_api::instruction::build_initialize_mint_ix;

use crate::context::Context;
use crate::error::Result;

/// Initialize the TAPE mint using the payer as authority.
///
/// Safe to call only once per cluster; subsequent calls will fail because the
/// mint PDA already exists.
pub async fn init(ctx: &Context) -> Result<()> {
    let authority = ctx.payer.pubkey().into();
    let ix = build_initialize_mint_ix(authority, authority);
    ctx.rpc.send_instructions(&ctx.payer, vec![ix]).await?;
    Ok(())
}
