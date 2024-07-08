import polars as pl
import logging
import asyncio
from math import ceil
from utils import column_dtypes
from flipside import Flipside
from flipside import errors as fs_errors

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class FSQuery:
    """
    Represents a query and result set to be executed on the Flipside API.
    The page size is fixed when creating the object so the amount of async tasks to fetch the results is known.
    Concurrency is set to 15 to address rate limiting.
    
    Args:
        conn (Flipside): The Flipside connection object.
        max_concurrent (int, optional): The maximum number of concurrent queries. Defaults to 15.
        page_size (int, optional): The number of rows to fetch per page. Defaults to 100000.
    """

    def __init__(self, conn: Flipside, max_concurrent:int=15, page_size:int=100000):
        self.conn = conn
        self.page_size = page_size
        self.sem = asyncio.Semaphore(max_concurrent)

        self.query_id = None
        self.result = None
        self.columns = None
        self.column_types = None
        self.total_rows = None
        self.total_pages = None
        self.rows = []

    def init_query(self, sql:str):
        """
        Runs the SQL statement query provided and retrieve the result metadata.

        Args:
            sql (str): The SQL statement to execute.
        """
        try:
            self.result = self.conn.query(sql, page_number=1, page_size=1, timeout_minutes=10)
        except fs_errors.QueryRunExecutionError as e:
            logger.error(f"Issue with query during execution: {e.message}")
            return
        except fs_errors.QueryRunTimeoutError as e:
            logger.error(f"Query took longer than 10 minutes to run: {e.message}")
            return
        self.columns = self.result.columns
        self.column_types = self.result.column_types
        self.query_id = self.result.query_id
        self.total_rows = self.result.page.totalRows
        self.total_pages = ceil(self.total_rows / self.page_size)
        self.rows = []

    async def fetch_paginated_results(self, page_number:int):
        """
        Fetches the results for a specific page number.

        Args:
            page_number (int): The page number to fetch.
        """
        if self.result is None:
            raise Exception("No initial query results found, please run init_query() first")

        async with self.sem:
            try:
                # Convert to thread to avoid blocking the event loop
                # Params are passed as kwargs
                results = await asyncio.to_thread(self.conn.get_query_results, **{
                    "query_run_id": self.query_id,
                    "page_number": page_number,
                    "page_size": self.page_size
                })
            except fs_errors.QueryRunRateLimitError as e:
                logger.error(f"Rate limit (15) was hit for page {page_number}: {e.message}")
                return
        if results.records:
            self.rows.extend(results.records)
            logger.info(f"Page number {page_number} retrieved {len(results.records)} rows.")

    def get_result_as_dataframe(self) -> pl.DataFrame:
        """
        Converts the query results to a Polars DataFrame.

        Returns:
            polars.DataFrame: The query results as a DataFrame.
        """
        df = pl.DataFrame(self.rows, schema=self.columns, orient='row')
        # Drop __row_index if present
        if '__row_index' in df:
            df.drop_in_place('__row_index')
        # Handle column dtypes
        df = df.cast(column_dtypes())
        # Handle timezones
        df = df.with_columns(pl.col(pl.Datetime).dt.replace_time_zone('UTC'))
        # Sort for duckDB performance
        df = df.sort('block_timestamp')

        return df
