use clap::ValueEnum;

/// Represents the different IOTA networks it is possible to download
/// checkpoints from.
#[derive(Debug, Clone, Copy, Default, ValueEnum, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum Network {
    #[default]
    Mainnet,
    Testnet,
    Devnet,
}

impl Network {
    /// Returns the URL of the network that stores the checkpoints.
    ///
    /// This URL is the base for the storages of historical and live
    /// checkpoints.
    fn base_checkpoints_url(&self) -> String {
        format!("https://checkpoints.{self}.iota.cafe")
    }

    /// Returns the URL of the network that stores the historical checkpoints.
    pub fn historical_checkpoints_url(&self) -> String {
        format!("{}/ingestion/historical", self.base_checkpoints_url())
    }

    /// Returns the URL of the network that hosts the key value data through
    /// REST API.
    pub fn rest_api_url(&self) -> String {
        format!("https://transactions.{self}.iota.cafe")
    }
}
