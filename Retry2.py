import functools
import time

def _attempt_q_call(connection, func_name, args, kwargs, chunk_dims, max_retries, retries):
    try:
        # Replace this with your actual IPC call:
        result = connection.call_q(func_name, *args, **kwargs)
        return result
    except Exception as e:
        # Check each candidate chunk dimension in order
        for chunk_dim in chunk_dims:
            if (chunk_dim in kwargs and isinstance(kwargs[chunk_dim], list) and 
                len(kwargs[chunk_dim]) > 1 and "too big" in str(e).lower()):
                items = kwargs[chunk_dim]
                mid = len(items) // 2
                print(f"Subdividing {func_name} on '{chunk_dim}': splitting {items} into {items[:mid]} and {items[mid:]}")
                # Create two new kwargs dictionaries for each subquery.
                kwargs1 = kwargs.copy()
                kwargs2 = kwargs.copy()
                kwargs1[chunk_dim] = items[:mid]
                kwargs2[chunk_dim] = items[mid:]
                res1 = _attempt_q_call(connection, func_name, args, kwargs1, chunk_dims, max_retries, retries)
                res2 = _attempt_q_call(connection, func_name, args, kwargs2, chunk_dims, max_retries, retries)
                # Assume results can be concatenated (adjust as needed)
                return res1 + res2
        # If no chunkable dimension was found or the error is not "too big", try a retry.
        if retries < max_retries:
            print(f"Retry {retries+1}/{max_retries} for {func_name} with kwargs={kwargs} due to error: {e}")
            time.sleep(1)
            return _attempt_q_call(connection, func_name, args, kwargs, chunk_dims, max_retries, retries + 1)
        else:
            print(f"Max retries exceeded for {func_name}.")
            raise

class QClient:
    def __init__(self, connection, config):
        """
        connection: your IPC connection to the kdb instance.
        config: mapping of q function names to their configuration, e.g.:
            {
                'getData': {
                    'chunk_dims': ['symbols'],  # For functions that have only symbols and a date.
                    'max_retries': 3
                },
                'getStats': {
                    'chunk_dims': ['symbols', 'date_range'],  # For functions that accept both.
                    'max_retries': 2
                }
            }
        """
        self.connection = connection
        self.config = config

    def __getattr__(self, func_name):
        cfg = self.config.get(func_name, {})
        chunk_dims = cfg.get("chunk_dims", [])
        max_retries = cfg.get("max_retries", 3)

        @functools.wraps(func_name)
        def q_func(*args, **kwargs):
            return _attempt_q_call(self.connection, func_name, args, kwargs, chunk_dims, max_retries, retries=0)
        return q_func

# Dummy connection simulating a kdb IPC interface.
class DummyKdbConnection:
    def call_q(self, func_name, *args, **kwargs):
        # Simulate a "too big" error based on the length of the chunkable parameter if present.
        # This is just an illustrative condition.
        for key in ['symbols', 'date_range', 'time_range']:
            if key in kwargs and isinstance(kwargs[key], list) and len(kwargs[key]) > 3:
                raise ValueError("Query too big")
        print(f"Executing {func_name} with args {args} and kwargs {kwargs}")
        # Simulated result.
        if 'symbols' in kwargs:
            return [{"result": f"{func_name} result for {sym}"} for sym in kwargs['symbols']]
        return [{"result": f"{func_name} result"}]

# Example usage:
if __name__ == '__main__':
    # Configuration for different q functions.
    q_config = {
        # A function with symbols and a date (single date, not a range)
        'getDataByDate': {
            'chunk_dims': ['symbols'],  
            'max_retries': 3
        },
        # A function with symbols and a date range.
        'getDataByDateRange': {
            'chunk_dims': ['symbols', 'date_range'],  
            'max_retries': 2
        },
        # A function with symbols, date range and time range.
        'getDataByDateTimeRange': {
            'chunk_dims': ['symbols', 'date_range', 'time_range'],  
            'max_retries': 2
        }
    }

    connection = DummyKdbConnection()
    client = QClient(connection, q_config)

    # Example: function with symbols and a single date.
    try:
        result = client.getDataByDate(
            symbols=['AAPL', 'GOOG', 'MSFT', 'AMZN', 'FB', 'TSLA'],
            date="2025-01-01"
        )
        print("\nResult for getDataByDate:")
        print(result)
    except Exception as ex:
        print(f"Error in getDataByDate: {ex}")

    # Example: function with symbols and a date range.
    try:
        result = client.getDataByDateRange(
            symbols=['AAPL', 'GOOG', 'MSFT', 'AMZN', 'FB', 'TSLA'],
            date_range=["2025-01-01", "2025-01-31"]
        )
        print("\nResult for getDataByDateRange:")
        print(result)
    except Exception as ex:
        print(f"Error in getDataByDateRange: {ex}")

    # Example: function with symbols, date range and time range.
    try:
        result = client.getDataByDateTimeRange(
            symbols=['AAPL', 'GOOG', 'MSFT', 'AMZN', 'FB', 'TSLA'],
            date_range=["2025-01-01", "2025-01-31"],
            time_range=["09:30", "16:00"]
        )
        print("\nResult for getDataByDateTimeRange:")
        print(result)
    except Exception as ex:
        print(f"Error in getDataByDateTimeRange: {ex}")
