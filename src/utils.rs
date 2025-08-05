use iota_kvstore::{Checkpoint, TransactionData};
use iota_types::{full_checkpoint_content::CheckpointData, object::Object};

/// Extracts data needed for assertion from a checkpoint.
pub fn extract_data_from_checkpoint(
    checkpoint: CheckpointData,
) -> (Vec<Object>, Vec<TransactionData>, Checkpoint) {
    let total_objects = checkpoint
        .transactions
        .iter()
        .map(|ch_tx| ch_tx.output_objects.len())
        .sum::<usize>();

    let mut objects = Vec::with_capacity(total_objects);
    let mut transactions = Vec::with_capacity(checkpoint.transactions.len());

    for transaction in checkpoint.transactions {
        transactions.push(TransactionData::new(
            &transaction,
            checkpoint.checkpoint_summary.sequence_number,
        ));
        objects.extend(transaction.output_objects);
    }

    let checkpoint = Checkpoint {
        contents: checkpoint.checkpoint_contents,
        summary: checkpoint.checkpoint_summary,
    };

    (objects, transactions, checkpoint)
}
