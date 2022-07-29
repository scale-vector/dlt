import os
import tempfile
from typing import Iterator, TypedDict, cast, Union

from dlt.common import json
from dlt.common.runners.venv import Venv
from dlt.common.sources import with_table_name
from dlt.common.typing import DictStrAny, StrAny, StrOrBytesPath

from examples.sources.stdout import get_source as get_singer_pipe

# from dlt.pipeline.exceptions import MissingDependencyException
FilePathOrDict = Union[StrAny, StrOrBytesPath]


class SingerMessage(TypedDict):
    type: str  # noqa: A003


class SingerRecord(SingerMessage):
    record: DictStrAny
    stream: str


class SingerState(SingerMessage):
    value: DictStrAny

# try:
#     from singer import parse_message_from_obj, Message, RecordMessage, StateMessage
# except ImportError:
#     raise MissingDependencyException("Singer Source", ["python-dlt-singer"], "Singer runtime compatible with DLT")


# pip install ../singer/singer-python
# https://github.com/datamill-co/singer-runner/tree/master/singer_runner
# https://techgaun.github.io/active-forks/index.html#singer-io/singer-python
def get_source_from_stream(singer_messages: Iterator[SingerMessage], state: DictStrAny = None) -> Iterator[DictStrAny]:
    last_state = {}
    for msg in singer_messages:
        if msg["type"] == "RECORD":
            # yield record
            msg = cast(SingerRecord, msg)
            yield with_table_name(msg["record"], msg["stream"])
        if msg["type"] == "STATE":
            msg = cast(SingerState, msg)
            last_state = msg["value"]
    if state:
        state["singer"] = last_state


def get_source(venv: Venv, tap_name: str, config_file: FilePathOrDict, catalog_file: FilePathOrDict, state: DictStrAny = None) -> Iterator[DictStrAny]:

    def as_config_file(config: FilePathOrDict) -> StrOrBytesPath:
        if type(config) is dict:
            fd, tmp_name = tempfile.mkstemp(dir=venv.context.env_dir)
            with os.fdopen(fd, "w") as f:
                json.dump(config, f)
            return cast(str, tmp_name)
        else:
            return config  # type: ignore

    # write config dictionary to temp file in virtual environment if passed as dict
    config_file_path = as_config_file(config_file)

    # process catalog like config
    catalog_file_path = as_config_file(catalog_file)

    # possibly pass state
    if state and state.get("singer"):
        state_params = ("--state", as_config_file(state["singer"]))
    else:
        state_params = ()  # type: ignore

    pipe_iterator = get_singer_pipe(venv,
                                    tap_name,
                                    "--config",
                                    os.path.abspath(config_file_path),
                                    "--catalog",
                                    os.path.abspath(catalog_file_path),
                                    *state_params
                                    )
    yield from get_source_from_stream(pipe_iterator, state)  # type: ignore
