use clap::Parser;

use condrun::cli::{Cli, dispatch};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("condrun=info")),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    // Use try_parse so we can map clap's default exit code (2) to condrun's
    // CLI-parse-error code (4) per SPEC §6.
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            // clap's Error::print already includes formatting (color, usage,
            // etc.) — write it directly. Errors that are clap "displayed help"
            // or "displayed version" still come through here; honor clap's
            // intended exit code (0) for those.
            let _ = e.print();
            let code = match e.kind() {
                clap::error::ErrorKind::DisplayHelp
                | clap::error::ErrorKind::DisplayVersion
                | clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => 0,
                _ => 4,
            };
            std::process::exit(code);
        }
    };

    match dispatch(cli).await {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            tracing::error!("{e:#}");
            std::process::exit(4);
        }
    }
}
