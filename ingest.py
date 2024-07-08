import os
import asyncio
import logging
import duckdb
import time
from query import FSQuery
from flipside import Flipside
from dotenv import load_dotenv
from utils import get_api_key

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

async def main():
    logger.info("Fetching API key...")
    key = get_api_key()

    logger.info("Creating connection to Flipside API...")
    flipside = Flipside(key, os.getenv('API_URL'))
    page_size = int(os.getenv('PAGE_SIZE', 100000))
    fs = FSQuery(flipside, page_size=page_size)

    low_ts, high_ts = os.getenv('LOW_TS'), os.getenv('HIGH_TS')
    if low_ts is None or high_ts is None:
        logger.error("Please provide a low and high timestamp in the environment variables.")
        return

    logger.info("Running query...")
    start_time = time.time()
    fs.init_query(f"""
        SELECT 
            fact_transfers_id,
            block_timestamp,
            block_id,
            tx_id,
            index,
            tx_from,
            tx_to,
            amount,
            mint,
            inserted_timestamp,
            modified_timestamp
        FROM SOLANA.CORE.FACT_TRANSFERS
        WHERE block_timestamp BETWEEN '{low_ts}' AND '{high_ts}'
    """)
    logger.info(f"--- Finished in {(time.time() - start_time):.2f} seconds ---")
    logger.info(f"Query will return {fs.total_rows} rows across {fs.total_pages} pages")

    logger.info("Fetching results...")
    start_time = time.time()
    tasks = set()
    for i in range(1, fs.total_pages+1):
        task = asyncio.create_task(fs.fetch_paginated_results(i))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
    await asyncio.gather(*tasks)
    logger.info(f"--- Finished in {(time.time() - start_time):.2f} seconds ---")

    logger.info("Outputting Parquet...")
    start_time = time.time()
    df = fs.get_result_as_dataframe()
    df.write_parquet('output.parquet')
    logger.info(f"--- Finished in {(time.time() - start_time):.2f} seconds ---")

    logger.info("Loading parquet object directly to Duckdb...")
    db_file = os.getenv('DB_FILE', 'solana.duckdb')
    start_time = time.time()
    con = duckdb.connect(database=db_file, read_only=False)
    con.query("""
        INSERT INTO transfers
        SELECT * FROM df
        ON CONFLICT DO NOTHING
    """)
    logger.info(f"--- Finished in {(time.time() - start_time):.2f} seconds ---")


if __name__ == '__main__':
    load_dotenv()
    asyncio.run(main())
