# Developing a contrib source from own source

We develop a source that we intend to share with others via the contrib repo. Let's assume that we convert the own source from `develop_own_source` with the lowest possible effort. We are going to reuse our **code** and developed **schema**. There are more professional ways to write a source: typically when you start from scratch or you do it professionally.

Here the objective is to make the own source re-usable, going beyond sharing just the snippet

## Pipeline init

Please read READMEs for `no_code_consume_source` and `consume_source` on options to initialize pipeline. In this case we do
```
dlt init contrib-source spotify
```

This will create
* development pipeline in `spotify_dev_pipeline.py` which is used to develop and test the source but is not intended to go to production! Still the iterative process to create the source is the same. See README in `developing_own_source` for more details
* `spotify` folder with contrib source structure (see folder). The `__init__` and `spotify_source.py` will be created by `dlt init` and contain basic skeleton for the source
* `tests` folder where you should place your tests and mock data
* `source_requirements.txt` where you can put additional dependencies for the source

## Contrib source code
We re-use code from the demo with minimal modifications.

1. Already developed `schema.yml` is used as source schema
2. The code stays intact, except doing `extract` we yield the data (see code, it's simple)
3. We provide minimal number of hints, instead relying on the already developed schema!


## Working iteratively on schema

We can still work with the schema iteratively if the option `sync_schema_folder` is enabled. We could even make the dlt to overwrite your `schema.yml` in `spotify` folder but that I would assume does not make much sense - I'd rather overwrite the schema manually when I'm happy with the results

## Requirements on contrib sources

We should strike some balance between the quality and easiness of contribition. My current take:
1. additional python dependencies are defined
2. the development pipeline runs in dry run mode with mock data (and on our sandbox if we have it)
3. the code structure is such that source can be automatically instantiated and loaded (in this case it would be `dlt load --to bigquery sources.spotify --data-dir data`)
4. there is enough documentation


## Sharing culture - what we can share here?

My current take is that this whole structure:
1. source itself that is importable by others
2. tests and mock data
3. development/example pipeline
4. source requirements should be shared.

both separate repo or part of the contrib are ok.


