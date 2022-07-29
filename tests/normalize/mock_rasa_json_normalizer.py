from dlt.common.normalizers.json import TNormalizedRowIterator
from dlt.common.schema import Schema
from dlt.common.normalizers.json.relational import normalize_data_item as relational_normalize, extend_schema
from dlt.common.sources import with_table_name
from dlt.common.typing import TDataItem


def normalize_data_item(schema: Schema, source_event: TDataItem, load_id: str) -> TNormalizedRowIterator:
    if schema.schema_name == "event":
        # this emulates rasa parser on standard parser
        event = {"sender_id": source_event["sender_id"], "timestamp": source_event["timestamp"]}
        yield from relational_normalize(schema, event, load_id)
        # add table name which is "event" field in RASA OSS
        with_table_name(source_event, "event_" + source_event["event"])

    # will generate tables properly
    yield from relational_normalize(schema, source_event, load_id)
