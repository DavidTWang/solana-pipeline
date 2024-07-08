from polars import Int64, Float64, Datetime, String


def get_api_key() -> str:
    """
    Retrieves the API key from the 'api.key' file.


    Returns:
        str: The API key.
    """
    with open('api.key', 'r') as f:
        return f.read()


def column_dtypes() -> dict:
    """
    Returns a dictionary mapping column names to their corresponding data types.

    Returns:
        dict: A dictionary where the keys are column names and the values are data types.
    """
    return {
        "fact_transfers_id": String,
        "block_timestamp": Datetime,
        "block_id": Int64,
        "tx_id": String,
        "index": String,
        "tx_from": String,
        "tx_to": String,
        "amount": Float64,
        "mint": String,
        "inserted_timestamp": Datetime,
        "modified_timestamp": Datetime,
    }
