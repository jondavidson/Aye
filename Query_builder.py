import datetime
import numpy as np
from typing import Any, Dict, List, Optional, Union

# Try to import Polars. If not installed, pl will be None.
try:
    import polars as pl
except ImportError:
    pl = None

class QSymbol:
    """
    Represents a q symbol literal.
    
    For example, QSymbol("abc") becomes the q literal `abc.
    """
    __match_args__ = ("value",)

    def __init__(self, value: str) -> None:
        """
        Initialize a QSymbol instance.
        
        Args:
            value (str): The string representation of the symbol.
        """
        self.value: str = value

    def __str__(self) -> str:
        """
        Return the q symbol literal as a string.
        
        Returns:
            str: The q symbol literal (e.g. "`abc").
        """
        return f"`{self.value}"

    def __repr__(self) -> str:
        """
        Return a string representation of the QSymbol.
        
        Returns:
            str: Representation of the QSymbol.
        """
        return f"QSymbol({self.value})"


class QQueryBuilder:
    """
    A builder for constructing valid q string queries from Python input types.
    
    Supported types include:
      • QSymbol: rendered as a q symbol literal (e.g. `sym)
      • str: rendered as a quoted string with embedded quotes escaped
      • bool: True → 1b, False → 0b
      • int, float: rendered using str()
      • list/tuple: rendered as space‑separated q literals
      • dict: rendered as "keys!values" (both keys and values are space‑separated)
      • None: rendered as 0N
      • datetime.datetime: rendered as "YYYY.MM.DDDHH:MM:SS.mmm" (with a D separator)
      • datetime.date: rendered as "YYYY.MM.DD"
      • datetime.time: rendered as "HH:MM:SS.mmm"
      • datetime.timedelta: rendered as a q timespan literal ("hh:mm:ss.mmm")
      • NumPy scalars (np.generic): converted to native Python types
      • NumPy arrays (np.ndarray): converted to lists recursively
      • Polars DataFrame: converted to a dict of columns (if polars is available)
      • Polars Series: converted to a list
      • Other types: fall back to str(obj)
      
    Note: This implementation does not (yet) support types like sets, frozensets, or other
          custom objects that require special formatting.
    """
    def __init__(self, function_name: str, params: Optional[List[Any]] = None) -> None:
        """
        Initialize a QQueryBuilder instance.
        
        Args:
            function_name (str): The q function name to be called.
            params (Optional[List[Any]]): A list of parameters to pass to the function.
        """
        self.function_name: str = function_name
        self.params: List[Any] = params if params is not None else []

    def build(self) -> str:
        """
        Build and return the q string query.
        
        If no parameters are provided, returns just the function name.
        Otherwise, returns the function call with parameters in the form:
            function_name[param1; param2; ...]
        
        Returns:
            str: The constructed q query string.
        """
        if not self.params:
            return self.function_name
        params_str: str = "; ".join(self.qrepr(p) for p in self.params)
        return f"{self.function_name}[{params_str}]"

    @classmethod
    def qrepr(cls, obj: Any) -> str:
        """
        Recursively convert a Python object into a q literal string.
        
        Uses structural pattern matching (match/case) to handle various types.
        
        Args:
            obj (Any): The Python object to be converted.
            
        Returns:
            str: The q literal representation of the object.
        """
        match obj:
            case QSymbol(value):
                return f"`{value}"
            case bool(b):
                return "1b" if b else "0b"
            case str():
                # Escape embedded double quotes
                escaped: str = obj.replace('"', '\\"')
                return f'"{escaped}"'
            case int() | float():
                return str(obj)
            case list() | tuple():
                # Render lists/tuples as space-separated q literals.
                return " ".join(cls.qrepr(item) for item in obj)
            case dict():
                # Render as dictionary: keys!values (both as space-separated lists).
                keys: str = " ".join(cls.qrepr(k) for k in obj.keys())
                values: str = " ".join(cls.qrepr(v) for v in obj.values())
                return f"{keys}!{values}"
            case datetime.datetime():
                # Format as "YYYY.MM.DDDHH:MM:SS.mmm" with a D separator.
                s: str = obj.strftime("%Y.%m.%dD%H:%M:%S.%f")[:-3]
                return f'"{s}"'
            case datetime.date():
                # Render date as "YYYY.MM.DD".
                s: str = obj.strftime("%Y.%m.%d")
                return f'"{s}"'
            case datetime.time():
                # Render time as "HH:MM:SS.mmm".
                s: str = obj.strftime("%H:%M:%S.%f")[:-3]
                return f'"{s}"'
            case datetime.timedelta():
                # Convert timedelta to a q timespan literal ("hh:mm:ss.mmm").
                total_seconds: int = int(obj.total_seconds())
                hours, rem = divmod(total_seconds, 3600)
                minutes, seconds = divmod(rem, 60)
                millis: int = int(obj.microseconds / 1000)
                timespan_str: str = f"{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}"
                return f'"{timespan_str}"'
            # NumPy scalar types
            case _ as x if isinstance(x, np.generic):
                return str(x.item())
            # NumPy arrays
            case _ as x if isinstance(x, np.ndarray):
                return cls.qrepr(x.tolist())
            # Polars DataFrame
            case _ as x if pl is not None and isinstance(x, pl.DataFrame):
                # Convert to a dictionary of columns (assumed to be lists).
                return cls.qrepr(x.to_dict(False))
            # Polars Series
            case _ as x if pl is not None and isinstance(x, pl.Series):
                return cls.qrepr(x.to_list())
            case None:
                return "0N"
            case _:
                # Fallback: use str() conversion.
                return str(obj)


# Example usage:
if __name__ == "__main__":
    # Create some example data.
    np_array: np.ndarray = np.array([10, 20, 30])
    np_scalar: np.int64 = np.int64(99)
    
    polars_df: Optional[Any] = None
    polars_series: Optional[Any] = None
    if pl is not None:
        polars_df = pl.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"]
        })
        polars_series = pl.Series("s", [True, False, True])
    
    builder = QQueryBuilder("myFunc", [
        42,
        3.14,
        'hello "world"',
        True,
        QSymbol("testSym"),
        [1, 2, 3],
        {"a": 1, "b": "text"},
        None,
        datetime.datetime(2021, 7, 5, 12, 34, 56, 789000),
        datetime.date(2021, 7, 5),
        datetime.time(12, 34, 56, 789000),
        datetime.timedelta(hours=1, minutes=23, seconds=45, microseconds=123000),
        np_array,
        np_scalar,
        polars_df,       # Only included if Polars is installed.
        polars_series    # Only included if Polars is installed.
    ])
    q_query: str = builder.build()
    print("Constructed q query:")
    print(q_query)
