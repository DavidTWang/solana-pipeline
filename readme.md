# Solana ingest pipeline

## Description

A basic Python script part of a data pipeline that queries and retrieves Solana transfer data from the Flipside Crypto API and stores it into a DuckDB instance.

## Installation

Requirements: Python 3.9+

1. Create virtualenv
2. Install Dependencies via requirements.txt and pip: `pip install -f requirements.txt`.
3. Create a file in the root directory named `api.key` and place your Flipside API key within.
4. Initialize the DuckDB file database by running `python init_db.py`
5. Adjust paramenters in the `.env` file, especially for the `low_ts` and `high_ts` values, which represent the block timestamp range used for the query.

## Usage

- Run `python ingest.py`
- `verify_db.py` is provided with some simple queries against `solana.duckdb` to verify output

## Design decisions
- **DuckDB** was chosen as the database for its ease of deployment, unique features, and powerful analyics ability. A more traditional OLTP database like Postgres seemed unfitting for this data that is not expected to update, and in this scope, does not have relations.
- For the sake of testing (and requirements), the results dataframe is exported as a parquet but also directly loaded into DuckDB. Depending on the use case it may be a better and more performant experience to save only the parquet files and load them into DuckDB as needed. Folder structure based hive partitioning can also be applied.
- DuckDB bulk-insert performance drops massively when multiple indexes are applied on the table, therefore indexes are not used. The dataset is more suited for aggregated analysis rather than hyper-specific lookups so the benefits of traditional indexes are diminished.
    - DuckDB uses zonemaps/min-max indexes automatically  
- Polars is used over Pandas as it is more performant for doing transformations (as opposed to analytics)
- The script is designed to be run by a separate scheduler. The intiial query call is not asynchronous as it would only be run once and the subsequent API calls are dependent on its output

## Issues and Concerns

- The `fact_transfers_id` column contains nulls even though it should be coalesced. For testing, a composite primary key of `block_id`, `tx_id` and `index` is used (Same as the DBT surrogate key that should be `fact_transfers_id`)
- Initially designed to handle hourly ingestions, however some hours have over 2.2 million rows which will throw an `QueryRunExecutionError` due to the result set being greater than 1GB. There are several potential ways to improve this:
    - Reduce the number of columns: Columns such as `modified_timestamp` and `fact_transfers_id` could be omitted, as the data is not expected to be changed and `fact_transfers_id` could be replaced or hashed locally (as it is unreliable anyways)
    - Reduce the low and high timestamps to 15 or 30 minutes, which during testing has not exceeded 2 million rows yet. But remains for further testing
    - Set a `LIMIT` on the query of around 2 million rows, and generate another query based on the timestamp output of the first one to capture the remaining missing rows.
- Not entirely certain if the rate limit of 15 queries applies to fetching query results, but a semaphor was put in place, controlled by the `max_concurrent` param in the FSQuery class
