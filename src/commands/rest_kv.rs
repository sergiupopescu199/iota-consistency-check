use std::sync::Arc;

use iota_storage::{
    http_key_value_store::HttpKVStore, key_value_store_metrics::KeyValueStoreMetrics,
};
use tracing::info;

use crate::{historical_checkpoint_reader::HistoricalCheckpointReader, types::Network, utils};

pub async fn run(network: Network, checkpoints: Vec<usize>) -> anyhow::Result<()> {
    let metrics = KeyValueStoreMetrics::new_for_tests();
    let rest_kv = HttpKVStore::new(&network.rest_api_url(), 100, metrics).map(Arc::new)?;

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
        validate::checkpoint_consistency(rest_kv.clone(), &checkpoint, &transactions, &objects)
            .await?;
        info!("validation completed successfully");
    }

    info!("All checkpoints are consistent",);

    Ok(())
}

mod validate {
    use std::{collections::HashSet, sync::Arc, time::Duration};

    use anyhow::{anyhow, bail};
    use backoff::{ExponentialBackoff, backoff::Backoff};
    use futures::{StreamExt, stream};
    use iota_kvstore::{Checkpoint, TransactionData};
    use iota_storage::{
        http_key_value_store::HttpKVStore, key_value_store::TransactionKeyValueStoreTrait,
    };
    use iota_types::{
        base_types::{ObjectID, SequenceNumber},
        digests::TransactionDigest,
        effects::{TransactionEffects, TransactionEvents},
        object::Object,
        transaction::Transaction,
    };
    use tracing::{error, info, instrument};

    use crate::commands::MAX_ITEMS_TO_FETCH;

    const CHUNK_SIZE: usize = 100;

    /// Validates complete consistency between checkpoint data and the KV REST
    /// API.
    ///
    /// This is the main orchestration function that performs comprehensive
    /// validation of all checkpoint components (objects, transactions, and
    /// checkpoint metadata) against their corresponding entries in the
    /// BigTable KV store through the KV REST API.
    #[instrument(skip_all, fields(objects_count = chk_objects.len(), transactions_count = chk_transactions.len()))]
    pub(super) async fn checkpoint_consistency(
        client: Arc<HttpKVStore>,
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

    /// Validates object consistency between checkpoint data and the KV REST
    /// API.
    ///
    /// This function fetches all objects from the BigTable KV store through the
    /// KV REST API that correspond to the objects present in the checkpoint
    /// data, then performs a comprehensive comparison to ensure data
    /// consistency.
    async fn objects(client: Arc<HttpKVStore>, objects: Vec<Object>) -> anyhow::Result<()> {
        let mut kv_store_objs = HashSet::<Object>::from_iter(objects.iter().cloned());

        let mut stream = stream::iter(objects.into_iter().map(|object| {
            let client = Arc::clone(&client);
            fetch_object_with_retry(client, object.id(), object.version())
        }))
        .buffered(MAX_ITEMS_TO_FETCH);

        while let Some(kv) = stream.next().await {
            kv_store_objs.remove(&kv?);
        }

        info!("- Checking objects");
        if !kv_store_objs.is_empty() {
            bail!("Object validation failed - some object not found in REST API");
        }
        info!("- objects matches!");
        Ok(())
    }

    /// Fetches an object from the KV REST API with retry logic.
    ///
    /// It uses an exponential backoff strategy to retry failed requests. This
    /// is because the REST API may be temporarily unavailable or experiencing
    /// high load and may return an Option::None even tough the value is
    /// present, this behavior was only manifested for genesis checkpoint.
    async fn fetch_object_with_retry(
        client: Arc<HttpKVStore>,
        object_id: ObjectID,
        version: SequenceNumber,
    ) -> anyhow::Result<Object> {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(60));
        backoff.initial_interval = Duration::from_millis(100);
        backoff.current_interval = backoff.initial_interval;
        backoff.multiplier = 2.0;

        loop {
            let next_backoff = backoff.next_backoff().ok_or(anyhow!(
                "Exausted all backoff retries, some requested objects were not found in KV REST API"
            ))?;

            match client.get_object(object_id, version).await {
                Ok(Some(object)) => break Ok(object),
                Ok(None) => {
                    tokio::time::sleep(next_backoff).await;
                    continue;
                }
                Err(e) => {
                    error!("Failed to fetch object {object_id} from KV REST API: {e}, retrying...");
                    tokio::time::sleep(next_backoff).await;
                    continue;
                }
            }
        }
    }

    /// Fetches transactions from the KV REST API with retry logic.
    ///
    /// It uses an exponential backoff strategy to retry failed requests. This
    /// is because the REST API may be temporarily unavailable or experiencing
    /// high load and may return an Option::None even tough the value is
    /// present, this behavior was only manifested for genesis checkpoint.
    async fn fetch_transactions_with_retry(
        client: &HttpKVStore,
        tx_digests: &[TransactionDigest],
    ) -> anyhow::Result<(Vec<Transaction>, Vec<TransactionEffects>), anyhow::Error> {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(60));
        backoff.initial_interval = Duration::from_millis(100);
        backoff.current_interval = backoff.initial_interval;
        backoff.multiplier = 2.0;

        loop {
            let next_backoff = backoff
                    .next_backoff()
                    .ok_or(anyhow!("Exausted all backoff retries, some requested transactions were not found in KV REST API"))?;

            match client.multi_get(tx_digests, tx_digests).await {
                Ok((tx, effects)) => {
                    if tx.iter().any(Option::is_none) {
                        tokio::time::sleep(next_backoff).await;
                        continue;
                    }

                    if effects.iter().any(Option::is_none) {
                        tokio::time::sleep(next_backoff).await;
                        continue;
                    }

                    break Ok((
                        tx.into_iter().map(|t| t.unwrap()).collect(),
                        effects.into_iter().map(|eff| eff.unwrap()).collect(),
                    ));
                }
                Err(e) => {
                    error!("Failed to fetch transactions from KV REST API: {e}, retrying...");
                    tokio::time::sleep(next_backoff).await;
                    continue;
                }
            }
        }
    }

    /// Validates transaction consistency between checkpoint data and the KV
    /// REST API.
    ///
    /// This function fetches all transactions from the BigTable KV store
    /// through the KV REST API that correspond to the transactions present
    /// in the checkpoint data, then performs a comprehensive comparison to
    /// ensure data consistency.
    async fn transactions(
        client: Arc<HttpKVStore>,
        transactions: Vec<TransactionData>,
    ) -> anyhow::Result<()> {
        let tx_digests = transactions
            .iter()
            .map(|tx_data| tx_data.transaction.digest().to_owned())
            .collect::<Vec<TransactionDigest>>();

        let mut kv_transactions = Vec::<Transaction>::with_capacity(tx_digests.len());
        let mut kv_effects = Vec::<TransactionEffects>::with_capacity(tx_digests.len());
        let mut kv_events = Vec::<Option<TransactionEvents>>::with_capacity(tx_digests.len());

        for digests in tx_digests.chunks(CHUNK_SIZE) {
            let (tx, effects) = fetch_transactions_with_retry(&client, digests).await?;
            let tx_events = client.multi_get_events_by_tx_digests(&tx_digests).await?;
            kv_transactions.extend(tx);
            kv_effects.extend(effects);
            kv_events.extend(tx_events);
        }

        info!("- Checking transactions");
        let transactions_clone = transactions.clone();
        let is_valid = tokio::task::spawn_blocking(move || {
            transactions_clone
                .iter()
                .zip(kv_transactions.iter())
                .zip(kv_effects.iter())
                .zip(kv_events.iter())
                .all(|(((tx, kv_tx), kv_eff), kv_events)| {
                    tx.transaction == *kv_tx && tx.effects == *kv_eff && tx.events == *kv_events
                })
        })
        .await?;

        if !is_valid {
            bail!(
                "Tx validation failed - mismatch between checkpoint and REST API transaction events"
            );
        }
        info!("- transactions matches!");
        Ok(())
    }

    /// Validates checkpoint consistency between reference data and the KV
    /// store.
    ///
    /// This function fetches the checkpoint from the BigTable KV store
    /// through the KV REST API using the sequence number and performs a
    /// comprehensive comparison with the checkpoint to ensure data
    /// integrity.
    async fn checkpoint(client: Arc<HttpKVStore>, checkpoint: Checkpoint) -> anyhow::Result<()> {
        let (sum_by_seq, contents, sum_by_digest) = client
            .multi_get_checkpoints(
                &[checkpoint.summary.sequence_number],
                &[checkpoint.summary.sequence_number],
                &[checkpoint.summary.digest().to_owned()],
            )
            .await?;

        info!("- Checking checkpoint");
        let sum_by_seq_match = sum_by_seq
            .first()
            .and_then(Option::as_ref)
            .map(|envelope| {
                let digest_match = envelope.digest() == checkpoint.summary.digest();
                let summary_match = envelope.data() == checkpoint.summary.data();
                digest_match && summary_match
            })
            .unwrap_or_default();

        let sum_by_digest_match = sum_by_digest
            .first()
            .and_then(Option::as_ref)
            .map(|envelope| {
                let digest_match = envelope.digest() == checkpoint.summary.digest();
                let summary_match = envelope.data() == checkpoint.summary.data();
                digest_match && summary_match
            })
            .unwrap_or_default();

        let contents_match = contents
            .first()
            .and_then(Option::as_ref)
            .map(|contents| contents == &checkpoint.contents)
            .unwrap_or_default();

        if !sum_by_seq_match && !sum_by_digest_match && !contents_match {
            bail!(
                "Checkpoint validation failed - mismatch between expected and REST API checkpoint data"
            );
        }
        info!("- checkpoint matches!");
        Ok(())
    }
}
