from collections import abc
from functools import wraps
from typing import Any, Callable, Optional, Sequence, TypeVar, Union, TypedDict
try:
    from typing_extensions import ParamSpec
except ImportError:
    ParamSpec = lambda x: [x]  # type: ignore

from dlt.common import logger
from dlt.common.time import sleep
from dlt.common.typing import StrAny, TDataItem


# possible types of items yielded by the source
# 1. document (mapping from str to any type)
# 2. Iterable (ie list) on the mapping above for returning many documents with single yield
TItem = Union[TDataItem, Sequence[TDataItem]]
TBoundItem = TypeVar("TBoundItem", bound=TItem)
TDeferred = Callable[[], TBoundItem]

_TFunParams = ParamSpec("_TFunParams")

# name of dlt metadata as part of the item
DLT_METADATA_FIELD = "_dlt_meta"


class TEventDLTMeta(TypedDict, total=False):
    table_name: str  # a root table in which store the event


def append_dlt_meta(item: TBoundItem, name: str, value: Any) -> TBoundItem:
    if isinstance(item, abc.Sequence):
        for i in item:
            i.setdefault(DLT_METADATA_FIELD, {})[name] = value
    elif isinstance(item, dict):
        item.setdefault(DLT_METADATA_FIELD, {})[name] = value

    return item


def with_table_name(item: TBoundItem, table_name: str) -> TBoundItem:
    # normalize table name before adding
    return append_dlt_meta(item, "table_name", table_name)


def get_table_name(item: StrAny) -> Optional[str]:
    if DLT_METADATA_FIELD in item:
        meta: TEventDLTMeta = item[DLT_METADATA_FIELD]
        return meta.get("table_name", None)
    return None


def with_retry(max_retries: int = 3, retry_sleep: float = 1.0) -> Callable[[Callable[_TFunParams, TBoundItem]], Callable[_TFunParams, TBoundItem]]:

    def decorator(f: Callable[_TFunParams, TBoundItem]) -> Callable[_TFunParams, TBoundItem]:

        def _wrap(*args: Any, **kwargs: Any) -> TBoundItem:
            attempts = 0
            while True:
                try:
                    return f(*args, **kwargs)
                except Exception as exc:
                    if attempts == max_retries:
                        raise
                    attempts += 1
                    logger.warning(f"Exception {exc} in iterator, retrying {attempts} / {max_retries}")
                    sleep(retry_sleep)

        return _wrap

    return decorator


def defer_iterator(f: Callable[_TFunParams, TBoundItem]) -> Callable[_TFunParams, TDeferred[TBoundItem]]:

    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> TDeferred[TBoundItem]:
        def _curry() -> TBoundItem:
            return f(*args, **kwargs)
        return _curry

    return _wrap
