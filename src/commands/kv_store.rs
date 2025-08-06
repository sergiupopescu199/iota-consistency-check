use std::sync::Arc;

use iota_kvstore::BigTableClient;
use tracing::info;

use crate::{historical_checkpoint_reader::HistoricalCheckpointReader, types::Network, utils};

pub async fn run(
    network: Network,
    instance_id: String,
    column_family: String,
    checkpoints: Vec<usize>,
) -> anyhow::Result<()> {
    let client = BigTableClient::new_remote(
        instance_id,
        true,
        None,
        "iota-consistency-check".to_string(),
        column_family,
        None,
    )
    .await
    .map(Arc::new)?;

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
        validate::checkpoint_consistency(client.clone(), &checkpoint, &transactions, &objects)
            .await?;
        info!("validation completed successfully");
    }
    info!("All checkpoints are consistent");
    Ok(())
}

mod validate {
    use std::{collections::HashSet, sync::Arc};

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
        client: Arc<BigTableClient>,
        chk_checkpoint: &Checkpoint,
        chk_transactions: &[TransactionData],
        chk_objects: &[Object],
    ) -> anyhow::Result<()> {
        let objects_task = {
            let client = Arc::clone(&client);
            let chk_objects = chk_objects.to_vec();
            tokio::spawn(objects(client, chk_objects))
        };

        let transactions_task = {
            let client = Arc::clone(&client);
            let chk_transactions = chk_transactions.to_vec();
            tokio::spawn(transactions(client, chk_transactions))
        };

        let checkpoint_task = {
            let client = Arc::clone(&client);
            let chk_checkpoint = chk_checkpoint.clone();
            tokio::spawn(checkpoint(client, chk_checkpoint))
        };

        // Wait for all tasks to complete
        let (objects_result, transactions_result, checkpoint_result) =
            tokio::try_join!(objects_task, transactions_task, checkpoint_task)?;

        objects_result?;
        transactions_result?;
        checkpoint_result?;

        Ok(())
    }

    /// Validates object consistency between checkpoint data and the KV store.
    ///
    /// This function fetches all objects from the BigTable KV store that
    /// correspond to the objects present in the checkpoint data, then
    /// performs a comprehensive comparison to ensure data consistency.
    async fn objects(client: Arc<BigTableClient>, objects: Vec<Object>) -> anyhow::Result<()> {
        let mut client = (*client).clone();
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
        info!("- objects matches!");

        Ok(())
    }

    /// Validates transaction consistency between checkpoint data and the KV
    /// store.
    ///
    /// This function fetches all transactions from the BigTable KV store that
    /// correspond to the transactions present in the checkpoint data, then
    /// performs a comprehensive comparison to ensure data consistency.
    async fn transactions(
        client: Arc<BigTableClient>,
        transactions: Vec<TransactionData>,
    ) -> anyhow::Result<()> {
        let mut client = (*client).clone();
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

        if transactions.len() != kv_store_tx.len() {
            bail!(
                "Transactions validation failed - expected: {}, actual: {}",
                transactions.len(),
                kv_store_tx.len()
            );
        }

        let (expected_transactions, sorted_kv_tx) = tokio::task::spawn_blocking({
            let transactions = transactions.to_vec();
            move || {
                let mut expected = transactions;
                let mut kv_tx = kv_store_tx;

                expected.sort_by_key(|tx| tx.transaction.digest().to_owned());
                kv_tx.sort_by_key(|tx| tx.transaction.digest().to_owned());

                (expected, kv_tx)
            }
        })
        .await?;

        info!("- Checking transactions");

        // CPU-intensive comparison in spawn_blocking
        let transactions_match = tokio::task::spawn_blocking(move || {
            expected_transactions
                .iter()
                .zip(sorted_kv_tx)
                .all(|(expected, kv)| {
                    expected.effects == kv.effects
                        && expected.events == kv.events
                        && expected.transaction == kv.transaction
                        && expected.checkpoint_number == kv.checkpoint_number
                })
        })
        .await?;

        if !transactions_match {
            bail!(
                "Transactions validation failed - mismatch between checkpoint and KV store transactions"
            );
        }
        info!("- transactions matches!");

        Ok(())
    }

    /// Validates checkpoint consistency between reference data and the KV
    /// store.
    ///
    /// This function fetches the checkpoint from the BigTable KV store using
    /// the sequence number and performs a comprehensive comparison with the
    /// checkpoint to ensure data integrity.
    async fn checkpoint(client: Arc<BigTableClient>, checkpoint: Checkpoint) -> anyhow::Result<()> {
        let mut client = (*client).clone();
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
        info!("- checkpoint matches!");

        Ok(())
    }
}
