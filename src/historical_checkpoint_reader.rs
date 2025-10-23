use std::{collections::BTreeSet, num::NonZeroUsize};

use anyhow::{Context, bail};
use iota_config::{
    node::ArchiveReaderConfig,
    object_storage_config::{ObjectStoreConfig, ObjectStoreType},
};
use iota_data_ingestion_core::history::reader::HistoricalReader;
use iota_types::full_checkpoint_content::CheckpointData;

use crate::types::Network;

/// Reader for historical checkpoints.
///
/// Reads batches of checkpoints from publicly available objects stores hosted
/// by the IOTA foundation.
pub struct HistoricalCheckpointReader {
    reader: HistoricalReader,
}

impl HistoricalCheckpointReader {
    pub async fn new(network: Network) -> anyhow::Result<Self> {
        let config = ArchiveReaderConfig {
            download_concurrency: NonZeroUsize::new(100)
                .expect("batch size must be greater than zero"),
            remote_store_config: ObjectStoreConfig {
                object_store: Some(ObjectStoreType::S3),
                object_store_connection_limit: 20,
                aws_endpoint: Some(network.historical_checkpoints_url()),
                aws_virtual_hosted_style_request: true,
                no_sign_request: true,
                ..Default::default()
            },
            use_for_pruning_watermark: false,
        };
        let reader = HistoricalReader::new(config)?;
        reader.sync_manifest_once().await?;

        Ok(HistoricalCheckpointReader { reader })
    }

    /// Fetch a range of checkpoints from the historical object store.
    pub async fn fetch_historical(
        &mut self,
        checkpoints: &[usize],
    ) -> anyhow::Result<Vec<CheckpointData>> {
        let manifest = self.reader.get_manifest().await;

        let files = self.reader.verify_and_get_manifest_files(manifest)?;

        let first_checkpoint = checkpoints
            .first()
            .context("Failed to get first checkpoint from non-empty list")?;
        let last_checkpoint = checkpoints
            .last()
            .context("Failed to get last checkpoint from non-empty list")?;

        let start_index = match files
            .binary_search_by_key(first_checkpoint, |s| s.checkpoint_seq_range.start as usize)
        {
            Ok(index) => index,
            Err(index) => index - 1,
        };

        let end_index = match files
            .binary_search_by_key(last_checkpoint, |s| s.checkpoint_seq_range.start as usize)
        {
            Ok(index) => index,
            Err(index) => index,
        };

        let mut downloaded_checkpoints = Vec::with_capacity(checkpoints.len());

        for metadata in files
            .into_iter()
            .enumerate()
            .filter_map(|(index, metadata)| {
                (index >= start_index && index <= end_index).then_some(metadata)
            })
        {
            let fetched_checkpoints = self
                .reader
                .iter_for_file(metadata.file_path())
                .await?
                .filter(|c| checkpoints.contains(&(c.checkpoint_summary.sequence_number as usize)));

            downloaded_checkpoints.extend(fetched_checkpoints);
        }

        let downloaded_seqs = downloaded_checkpoints
            .iter()
            .map(|c| c.checkpoint_summary.sequence_number as usize)
            .collect::<BTreeSet<_>>();

        let mut requested = BTreeSet::from_iter(checkpoints.iter().copied());

        for seq in &downloaded_seqs {
            requested.remove(seq);
        }

        if !requested.is_empty() {
            bail!("historical is missing checkpoints: {:?}", requested);
        }

        Ok(downloaded_checkpoints)
    }

    /// Check if all requested checkpoints are available in the historical
    /// object store.
    pub async fn check_for_available_checkpoints(
        &self,
        checkpoints: &[usize],
    ) -> anyhow::Result<()> {
        let latest_available_checkpoint = self.reader.latest_available_checkpoint().await?;
        let all_requested_checkpoints_present = checkpoints
            .last()
            .map(|lates_checkpoint_to_check| {
                lates_checkpoint_to_check <= &(latest_available_checkpoint as usize)
            })
            .unwrap_or_default();

        if !all_requested_checkpoints_present {
            bail!(
                "Requested checkpoints exceeds the latest available checkpoint {latest_available_checkpoint}"
            )
        }

        Ok(())
    }
}
