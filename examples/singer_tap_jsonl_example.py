
from typing import Iterator
from dlt.pipeline import Pipeline
from dlt.pipeline.typing import GCPPipelineCredentials

from examples.sources.singer_tap import SingerMessage, get_source_from_stream
from examples.sources.jsonl import get_source as read_jsonl

# load hubspot schema stub - it converts all field names with `timestamp` into timestamp type
schema = Pipeline.load_schema_from_file("examples/schemas/hubspot_schema.yml")
p = Pipeline("singer_tap_hubspot")
p.create_pipeline(GCPPipelineCredentials.from_services_file("_secrets/project1234_service.json", "load_3"), schema=schema)
# p.extract(get_source(read_jsonl("examples/data/singer_taps/tap_google_sheet.jsonl")))
raw_messages: Iterator[SingerMessage] = read_jsonl("examples/data/singer_taps/tap_hubspot.jsonl")  # type: ignore
p.extract(get_source_from_stream(raw_messages, p.state))
p.normalize()
# print(p.get_default_schema().as_yaml(remove_defaults=True))
p.load()
