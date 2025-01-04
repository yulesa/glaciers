use glaciers::abi_reader::{self, AbiReaderError};
use glaciers::configger::{set_config_toml, get_config, ConfiggerError};
use glaciers::decoder::{decode_log_folder, DecoderError};
use thiserror::Error;

#[derive(Error, Debug)]
enum AppError {
    #[error("Configger error: {0}")]
    ConfigError(#[from] ConfiggerError),
    #[error("ABI Reader error: {0}")]
    AbiError(#[from] AbiReaderError),
    #[error("Decoder error: {0}")]
    DecodeError(#[from] DecoderError),
}

#[tokio::main]
async fn main() {
    if let Err(err) = async_main().await {
        eprintln!("Error: {}", err);
        std::process::exit(1);
    }
}

async fn async_main() -> Result<(), AppError> {
    let config_path = "glaciers_config_edit_example.toml";
    set_config_toml(config_path)?;
    println!("{:?}", get_config());
    // Read ABI list
    abi_reader::update_abi_df(get_config().main.abi_df_file_path, get_config().main.abi_folder_path)?;

    // process the log files concurrently
    decode_log_folder(get_config().main.raw_logs_folder_path, get_config().main.abi_df_file_path).await?;
    Ok(())
}
