
from typing import Iterator, Tuple, Callable, TYPE_CHECKING

from dlt.common.typing import TDataItem, StrAny
if TYPE_CHECKING:
    from dlt.common.schema import Schema

# type definitions for json normalization function

# iterator of form ((table_name, parent_table), dict) must be returned from normalization function
TNormalizedRowIterator = Iterator[Tuple[Tuple[str, str], StrAny]]

# normalization function signature
TNormalizeJSONFunc = Callable[["Schema", TDataItem, str], TNormalizedRowIterator]
