# Pipeline that consume source
The project structure may be created as follows:

1. by cloning github template
2. with an empty folder - in that case we run on defaults
3. with `dlt init pipeline [pipeline_name]` see also README for `consume_source` case

In our case we do
```
dlt init consume spotify_pipeline
```

which will create folder structure and `spotify_pipeline.py` with necessary imports and maybe some code commented out...

## Running the pipeline

We have two options here

1. Run the pipeline explicity in the pipeline script
2. Use `dlt run [script] --to bigquery --credentials bigquery_creds