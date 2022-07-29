from dlt.common import Decimal
from dlt.common.time import sleep
from dlt.common.typing import DictStrAny
from dlt.pipeline import Pipeline, GCPPipelineCredentials
from dlt.common.arithmetics import numeric_default_context, numeric_default_quantize

from examples.schemas.ethereum import discover_schema
from examples.sources.ethereum import get_source

credentials = GCPPipelineCredentials.from_services_file("_secrets/project1234_service.json", "mainnet_6")
# credentials = PostgresPipelineCredentials("redshift", "chat_analytics_rasa", "mainnet_6", "loader", "3.73.90.3")

pipeline = Pipeline("ethereum")
pipeline.create_pipeline(credentials, schema=discover_schema())
print(pipeline.root_path)

# get ethereum source which is python iterator, request 3 newest blocks with default lag. also pass the state (optional)
# so you can get newly produced blocks when the pipeline is run again
i = get_source("https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161", 2, state=pipeline.state)


# you can transform the blocks by defining mapping function
def from_wei_to_eth(block: DictStrAny) -> DictStrAny:
    # convert wei to eth providing with numeric precision at the target
    with numeric_default_context():
        for tx in block["transactions"]:
            v_wei = tx["value"]
            if v_wei > 0:
                # 9 scale vs 18 digits ether scale: a no go for any financial operations
                # TODO: better support for decimal types
                tx["eth_value"] = numeric_default_quantize(Decimal(v_wei) / 10**18)
                # print(f"Produced nice decimal {tx['eth_value']}")
    return block


# extract data from the source. operation is atomic and like all atomic operations in pipeline, it does not raise but returns
# execution status
pipeline.extract(map(from_wei_to_eth, i), table_name="blocks")
print(pipeline.state)

# wait for ethereum network to produce some more blocks
sleep(20)

# restore the pipeline from the working directory (simulate continuation from the saved state)
pipeline.restore_pipeline(credentials, pipeline.root_path)
# obtain new iterator (the old one is expired), this time use deferred iterator to allow parallel block reading
i = get_source("https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161", 2, state=pipeline.state)
pipeline.extract(map(from_wei_to_eth, i), table_name="blocks")
print(pipeline.state)

# this will normalize and load all extracted data
pipeline.flush()
