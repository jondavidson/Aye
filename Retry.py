import functools
import time

def retry_and_subdivide(chunk_dim='symbols', max_retries=3):
    """
    Decorator that wraps a query function to catch errors and, if an error
    indicates the query is too big, subdivides the query on the specified dimension.
    For other errors, it retries the query up to max_retries times.
    
    Parameters:
      chunk_dim: the keyword argument (e.g. 'symbols') whose list value should be subdivided.
      max_retries: maximum number of retries before giving up.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return _attempt_query(func, args, kwargs, chunk_dim, max_retries, retries=0)
        return wrapper
    return decorator

def _attempt_query(func, args, kwargs, chunk_dim, max_retries, retries):
    try:
        # Attempt the query.
        return func(*args, **kwargs)
    except Exception as e:
        # Check if the error indicates the query is "too big"
        # and if the chunk_dim value is a list with more than one element.
        if "too big" in str(e).lower() and chunk_dim in kwargs and isinstance(kwargs[chunk_dim], list) and len(kwargs[chunk_dim]) > 1:
            items = kwargs[chunk_dim]
            mid = len(items) // 2
            print(f"Subdividing query for {chunk_dim}: splitting {items} into {items[:mid]} and {items[mid:]}")
            # Create copies of kwargs for each subquery.
            kwargs1 = kwargs.copy()
            kwargs2 = kwargs.copy()
            kwargs1[chunk_dim] = items[:mid]
            kwargs2[chunk_dim] = items[mid:]
            # Recursively attempt each subquery.
            result1 = _attempt_query(func, args, kwargs1, chunk_dim, max_retries, retries)
            result2 = _attempt_query(func, args, kwargs2, chunk_dim, max_retries, retries)
            # Combine results (assuming the function returns lists).
            return result1 + result2
        # For other errors, if we haven't hit the retry limit, try again.
        elif retries < max_retries:
            print(f"Retry {retries + 1}/{max_retries} for query with {chunk_dim}={kwargs.get(chunk_dim)} due to error: {e}")
            time.sleep(1)  # Optional delay before retrying.
            return _attempt_query(func, args, kwargs, chunk_dim, max_retries, retries + 1)
        else:
            print("Max retries exceeded. Raising exception.")
            raise

# Example query function simulating a kdb/IPC call.
@retry_and_subdivide(chunk_dim='symbols', max_retries=2)
def run_query(*args, **kwargs):
    """
    Simulated query function. Raises an error if the list of symbols is deemed too large.
    Expected kwargs include: symbols, start_date, end_date, start_time, and end_time.
    """
    symbols    = kwargs.get('symbols', [])
    start_date = kwargs.get('start_date')
    end_date   = kwargs.get('end_date')
    start_time = kwargs.get('start_time')
    end_time   = kwargs.get('end_time')
    
    # Simulate the "too big" condition.
    if isinstance(symbols, list) and len(symbols) > 3:
        raise ValueError("Query too big")
    
    print(f"Executing query for symbols: {symbols}, Date: {start_date} to {end_date}, Time: {start_time} to {end_time}")
    # Simulate a successful query result.
    return [{"symbol": s, "data": "dummy data"} for s in (symbols if isinstance(symbols, list) else [symbols])]

# Example usage:
if __name__ == '__main__':
    # A list of symbols that initially triggers the "too big" error.
    symbols_list = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'FB', 'TSLA']
    
    try:
        result = run_query(
            symbols=symbols_list,
            start_date="2025-01-01",
            end_date="2025-01-31",
            start_time="09:30",
            end_time="16:00"
        )
        print("\nCombined Query Result:")
        print(result)
    except Exception as ex:
        print(f"Final error: {ex}")
