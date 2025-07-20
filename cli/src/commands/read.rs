use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use num_enum::TryFromPrimitive;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tokio::{task, time::Duration};

use crate::cli::{Cli, Commands};
use crate::log;
use crate::utils::write_output;

use tape_client::{
    decode_tape, finalize_read, get_tape_account, init_read, process_next_block, MimeType,
    TapeHeader,
};

pub async fn handle_read_command(cli: Cli, client: RpcClient) -> Result<()> {
    if let Commands::Read { tape, output } = cli.command {
        let tape_address = Pubkey::from_str(&tape)
            .map_err(|_| anyhow::anyhow!("Invalid tape address: {}", tape))?;

        log::print_message("Reading tape...");
        log::print_divider();

        let pb = setup_progress_bar();

        pb.set_message("Fetching tape metadata...");
        let (tape, _) = get_tape_account(&client, &tape_address).await?;
        let header = TapeHeader::try_from_bytes(&tape.header)?;

        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.white/gray}] {pos}/{len} {wide_msg}")
                .expect("Failed to set progress style"),
        );
        pb.set_length(tape.total_segments);
        pb.set_position(0);
        pb.set_message("");

        let mut state = init_read(tape.tail_slot);

        while process_next_block(&client, &tape_address, &mut state).await? {
            pb.set_position(state.segments_len() as u64);
        }

        let data = finalize_read(state)?;
        let result = decode_tape(data, header)?;

        let mime_type_enum =
            MimeType::try_from_primitive(header.mime_type).unwrap_or(MimeType::Unknown);

        pb.finish();
        write_output(output, &result, mime_type_enum)?;

        log::print_divider();
    }
    Ok(())
}

fn setup_progress_bar() -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {wide_msg}")
            .expect("Failed to set progress style"),
    );

    let pb_clone = pb.clone();
    task::spawn(async move {
        while !pb_clone.is_finished() {
            pb_clone.tick();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    pb
}
