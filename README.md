# IOTA Consistency Check

Verify data consistency between IOTA network checkpoints and services that store checkpoint data into separate permanent storages. This tool ensures that data served by various IOTA infrastructure components (BigTable KV store, REST APIs) matches the canonical checkpoint data from IOTA networks.

## Overview

The IOTA Consistency Check tool validates data integrity across different storage layers in the IOTA ecosystem by:

- Fetching historical checkpoint data from the IOTA network
- Comparing objects, transactions, and checkpoint metadata across different data sources
- Implementing robust retry mechanisms to handle transient network issues
- Providing detailed validation reporting with tracing support

## Features

- **Multi-Network Support**: Works with Mainnet, Testnet, and Devnet
- **Multiple Validation Modes**:
  - BigTable KV Store validation
  - REST API validation
- **Comprehensive Validation**: Validates objects, transactions, transaction effects, events, and checkpoint metadata
- **Flexible Checkpoint Selection**: Support for individual checkpoints and ranges

## Installation

```bash
cargo build --release
```

## Usage

```bash
# Validate checkpoint 0 against REST API on mainnet
iota-consistency-check -n mainnet -c 0 rest-kv
# Validate checkpoint range 100-110 against REST API on testnet
iota-consistency-check -n testnet -c 100-110 rest-kv
# Use custom log level
LOG_LEVEL=DEBUG iota-consistency-check -n mainnet -c 0 rest-kv

# Validate against BigTable KV store (requires credentials for direct database access)
GOOGLE_APPLICATION_CREDENTIALS=testnet-credentials.json iota-consistency-check -n testnet -c 1,2,3-5,10 kv-store
```
