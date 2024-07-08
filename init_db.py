import duckdb
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main():
    conn = duckdb.connect('solana.duckdb')
    conn.sql("""
    CREATE TABLE IF NOT EXISTS transfers (
        fact_transfers_id STRING,
        block_timestamp TIMESTAMPTZ,
        block_id BIGINT,
        tx_id STRING,
        index STRING,
        tx_from STRING,
        tx_to STRING,
        amount DOUBLE,
        mint STRING,
        inserted_timestamp TIMESTAMPTZ,
        modified_timestamp TIMESTAMPTZ,
        PRIMARY KEY (block_id, tx_id, index)
    );                 
    """)

    # Disabled indexes due to performance issues during bulk insert
    # Better to create indexes after the data has been inserted
    # and only if needed. Performance gains are reportedly small for non-very specific queries.
    # conn.sql("""
    # CREATE INDEX IF NOT EXISTS b_ts_idx ON transfers (block_timestamp);
    # CREATE INDEX IF NOT EXISTS mint_idx ON transfers (mint);
    # CREATE INDEX IF NOT EXISTS ft_idx ON transfers (tx_from, tx_to);
    # """)

    logger.info("Database created successfully.")


if __name__ == "__main__":
    main()