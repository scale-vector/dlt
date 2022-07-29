from typing import Any, AnyStr, List, Literal, Optional, Tuple, TypeVar, TypedDict

from dlt.common.dataset_writers import TLoaderFileFormat


LoadJobStatus = Literal["running", "failed", "retry", "completed"]
# native connection
TNativeConn = TypeVar("TNativeConn", bound="object")


class TLoaderCapabilities(TypedDict):
    preferred_loader_file_format: TLoaderFileFormat
    supported_loader_file_formats: List[TLoaderFileFormat]


# type for dbapi cursor
class DBCursor:
    closed: Any
    connection: Any
    query: Any
    description: Tuple[Any, ...]

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any ) -> None:
        ...
    def fetchall(self) -> List[Tuple[Any, ...]]:
        ...
    def fetchmany(self, size: int = ...) -> List[Tuple[Any, ...]]:
        ...
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        ...
