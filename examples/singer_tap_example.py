import os
from tempfile import mkdtemp

from dlt.pipeline import Pipeline, GCPPipelineCredentials
from dlt.common.runners.venv import Venv

from examples.sources.singer_tap import get_source

p = Pipeline("singer_csv")
p.create_pipeline(GCPPipelineCredentials.from_services_file("_secrets/project1234_service.json", "load_1"), working_dir="_storage/pipeline_singer")

# create Venv with desired dependencies, in this case csv tap
# venv creation costs time so it should be created only once and reused

# here we use context manager to automatically delete venv after example was run
# the dependency is meltano version of csv tap
print("Spawning virtual environment to run singer and installing csv tap from git+https://github.com/MeltanoLabs/tap-csv.git")
# WARNING: on MACOS you need to have working gcc to use tap-csv, otherwise dependency will not be installed
with Venv.create(mkdtemp(), ["git+https://github.com/MeltanoLabs/tap-csv.git"]) as venv:
    # prep singer config for tap-csv
    csv_tap_config = {
        "files": [
            {
                "entity": "annotations_202205",
                "path": os.path.abspath("examples/data/singer_taps/model_annotations.csv"),
                "keys": [
                    "message id"
                ]
            }
        ]
    }
    print("running tap-csv")
    i = get_source(venv, "tap-csv", csv_tap_config, "examples/data/singer_taps/csv_catalog.json", state=p.state)
    p.extract(i)
    p.normalize()
    print(p.get_default_schema().as_yaml(remove_defaults=True))
    # p.load()
