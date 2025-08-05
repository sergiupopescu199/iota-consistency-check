use iota_kvstore::BigTableClient;
use tracing::info;

use crate::{historical_checkpoint_reader::HistoricalCheckpointReader, types::Network, utils};

pub async fn run(
    network: Network,
    instance_id: String,
    column_family: String,
    checkpoints: Vec<usize>,
) -> anyhow::Result<()> {
    let mut client = BigTableClient::new_remote(
        instance_id,
        true,
        None,
        "iota-consistency-check".to_string(),
        column_family,
        None,
    )
    .await?;

    let mut reader = HistoricalCheckpointReader::new(network).await?;
    reader.check_for_available_checkpoints(&checkpoints).await?;
    info!("Downloading checkpoints");
    let downloaded_checkpoints = reader.fetch_historical(&checkpoints).await?;
    info!("Check checkpoints consistency");

    for checkpoint in downloaded_checkpoints {
        let sequence_number = checkpoint.checkpoint_summary.sequence_number;

        let (objects, transactions, checkpoint) = utils::extract_data_from_checkpoint(checkpoint);
        let span = tracing::info_span!("Checkpoint", sequence_number,);
        let _enter = span.enter();

        info!("starting validation");
        validate::checkpoint_consistency(&mut client, &checkpoint, &transactions, &objects).await?;
        info!("validation completed successfully");
    }
    info!("All checkpoints are consistent");
    Ok(())
}

mod validate {
    use std::collections::HashSet;

    use anyhow::bail;
    use iota_kvstore::{BigTableClient, Checkpoint, KeyValueStoreReader, TransactionData};
    use iota_types::{digests::TransactionDigest, object::Object, storage::ObjectKey};
    use tracing::{info, instrument};

    use crate::commands::MAX_ITEMS_TO_FETCH;

    /// Validates complete consistency between checkpoint data and the KV store.
    ///
    /// This is the main orchestration function that performs comprehensive
    /// validation of all checkpoint components (objects, transactions, and
    /// checkpoint metadata) against their corresponding entries in the
    /// BigTable KV store.
    #[instrument(skip_all, fields(objects_count = chk_objects.len(), transactions_count = chk_transactions.len()))]
    pub(super) async fn checkpoint_consistency(
        client: &mut BigTableClient,
        chk_checkpoint: &Checkpoint,
        chk_transactions: &[TransactionData],
        chk_objects: &[Object],
    ) -> anyhow::Result<()> {
        objects(client, chk_objects).await?;
        transactions(client, chk_transactions).await?;
        checkpoint(client, chk_checkpoint).await
    }

    /// Validates object consistency between checkpoint data and the KV store.
    ///
    /// This function fetches all objects from the BigTable KV store that
    /// correspond to the objects present in the checkpoint data, then
    /// performs a comprehensive comparison to ensure data consistency.
    async fn objects(client: &mut BigTableClient, objects: &[Object]) -> anyhow::Result<()> {
        let mut kv_store_objs = HashSet::new();
        for obj_keys in objects
            .iter()
            .map(|o| ObjectKey(o.id(), o.version()))
            .collect::<Vec<ObjectKey>>()
            .chunks(MAX_ITEMS_TO_FETCH)
        {
            let fetched_objects = client.get_objects(obj_keys).await?;
            kv_store_objs.extend(fetched_objects);
        }

        info!("- Checking objects");
        if kv_store_objs != HashSet::from_iter(objects.iter().map(ToOwned::to_owned)) {
            bail!("Objects validation failed - mismatch between checkpoint and KV store objects");
        }

        Ok(())
    }

    /// Validates transaction consistency between checkpoint data and the KV
    /// store.
    ///
    /// This function fetches all transactions from the BigTable KV store that
    /// correspond to the transactions present in the checkpoint data, then
    /// performs a comprehensive comparison to ensure data consistency.
    async fn transactions(
        client: &mut BigTableClient,
        transactions: &[TransactionData],
    ) -> anyhow::Result<()> {
        let mut kv_store_tx = Vec::with_capacity(transactions.len());
        for tx_digests in transactions
            .iter()
            .map(|tx| tx.transaction.digest().to_owned())
            .collect::<Vec<TransactionDigest>>()
            .chunks(MAX_ITEMS_TO_FETCH)
        {
            let fetched_transactions = client.get_transactions(tx_digests).await?;
            kv_store_tx.extend(fetched_transactions);
        }

        let mut expected_transactions = transactions.to_vec();
        expected_transactions.sort_by_key(|tx| tx.transaction.digest().to_owned());
        kv_store_tx.sort_by_key(|tx| tx.transaction.digest().to_owned());

        if expected_transactions.len() != kv_store_tx.len() {
            bail!(
                "Transactions validation failed - expected: {}, actual: {}",
                expected_transactions.len(),
                kv_store_tx.len()
            );
        }

        info!("- Checking transactions");
        if !expected_transactions
            .iter()
            .zip(kv_store_tx)
            .all(|(expected, kv)| {
                expected.effects == kv.effects
                    && expected.events == kv.events
                    && expected.transaction == kv.transaction
                    && expected.checkpoint_number == kv.checkpoint_number
            })
        {
            bail!(
                "Transactions validation failed - mismatch between checkpoint and KV store transactions"
            );
        }

        Ok(())
    }

    /// Validates checkpoint consistency between reference data and the KV
    /// store.
    ///
    /// This function fetches the checkpoint from the BigTable KV store using
    /// the sequence number and performs a comprehensive comparison with the
    /// checkpoint to ensure data integrity.
    async fn checkpoint(
        client: &mut BigTableClient,
        checkpoint: &Checkpoint,
    ) -> anyhow::Result<()> {
        let kv_checkpoint = client
            .get_checkpoints(&[checkpoint.summary.sequence_number])
            .await?;

        info!("- Checking checkpoint");
        if !kv_checkpoint
            .first()
            .map(|kv| {
                kv.summary.digest() == checkpoint.summary.digest()
                    && kv.summary.data() == checkpoint.summary.data()
                    && kv.contents == checkpoint.contents
            })
            .unwrap_or_default()
        {
            bail!(
                "Checkpoint validation failed - mismatch between expected and KV stored checkpoint data"
            );
        }

        Ok(())
    }
}
