import logging
import duckdb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main():
    conn = duckdb.connect('solana.duckdb')
    logger.info(conn.sql("""
        SELECT mint, count(*)
        FROM transfers
        GROUP BY mint
        ORDER BY count(*) DESC
        LIMIT 10
    """))
    logger.info(conn.sql(
        """
        SELECT DATE_TRUNC('hour', block_timestamp) AS ts, count(*)
        FROM transfers
        GROUP BY ts
        ORDER BY ts DESC
        """
    ))

if __name__ == "__main__":
    main()