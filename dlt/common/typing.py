from collections.abc import Mapping as C_Mapping, Sequence as C_Sequence
from re import Pattern as _REPattern
from typing import Callable, Dict, Any, Literal, Mapping, NewType, Type, TypeVar, TypedDict, TYPE_CHECKING, Union, get_args, get_origin
if TYPE_CHECKING:
    from _typeshed import StrOrBytesPath
    from typing import _TypedDict
    REPattern = _REPattern[str]
else:
    StrOrBytesPath = Any
    from typing import _TypedDictMeta as _TypedDict
    REPattern = _REPattern

DictStrAny = Dict[str, Any]
DictStrStr = Dict[str, str]
StrAny = Mapping[str, Any]  # immutable, covariant entity
StrStr = Mapping[str, str]  # immutable, covariant entity
StrStrStr = Mapping[str, Mapping[str, str]]  # immutable, covariant entity
TFun = TypeVar("TFun", bound=Callable[..., Any])
TAny = TypeVar("TAny", bound=Any)
TSecretValue = NewType("TSecretValue", str)  # represent secret value ie. coming from Kubernetes/Docker secrets or other providers
TDataItem = DictStrAny


def is_optional_type(t: Type[Any]) -> bool:
    # todo: use typing get_args and get_origin in python 3.8
    if hasattr(t, "__origin__"):
        return t.__origin__ is Union and type(None) in t.__args__
    return False


def extract_optional_type(t: Type[Any]) -> Any:
    return get_args(t)[0]


def is_literal_type(hint: Type[Any]) -> bool:
    return hasattr(hint, "__origin__") and hint.__origin__ is Literal


def is_typeddict(t: Any) -> bool:
    return isinstance(t, _TypedDict)


def is_list_generic_type(t: Any) -> bool:
    try:
        o = get_origin(t)
        return issubclass(o, list) or issubclass(o, C_Sequence)
    except Exception:
        return False


def is_dict_generic_type(t: Any) -> bool:
    try:
        o = get_origin(t)
        return issubclass(o, dict) or issubclass(o, C_Mapping)
    except Exception:
        return False
