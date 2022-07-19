# Developing a pipeline with a custom (own) source
This is example of a pipeline that includes custom source. The pipeline will be developed interatively by letting the user to modify schemas and run it again to apply changes.
At the end we'll consume a few custom sources to show how pipeline with many sources could work.

## Pipeline init

Please read READMEs for `no_code_consume_source` and `consume_source` on options to initialize pipeline. In this case we do
```
dlt init custom-source spotify_pipeline
```

This will create `spotify_pipeline.py` configured for working with schemas iteratively. I also propose a `recreate_tables` config param that makes sure that tables are re-created with each run. (it may be also part of `load` method).

## Custom source code

This is exactly what we had in spotify demo.
1. First we generate the data
2. Then we pass the data to `extract` method. The extract method takes additional parameters that let you define the schema for the passed data.

Examples:

- table name. if not provided: `table` will be used
- write_disposition, merge_keys, columns=[...], parent, - you can define a complete table schema here
- schema - string or object: you can pass a ready schema or a schema name in whcih tables will be created

## Working iteratively on schema

1. you start with empty `schemas` folder
2. you provide sensible and minimal setting for your tables into `extract` method
3. you run your script
4. in `schemas` folder you see inferred schemas: from your hints in `extract` and from the data itself
5. modify the schemas and run your script again
6. remove the `dry_run` option to actually load data
7. your source is ready. now you can add another one or some custom source and you are done with the pipeline

## Preventing unnecessary data extraction and creation of mock data

If you extract a lot of data just to iterate on the schema, it would be trivial to keep your source data and just infer schema and load it several times. So let's add `cache_source_data=True` to our config and your extraction will run only once!

**we can also export this data as mock data to let people test the source themselves** ie.

```python
p.extract(....)
p.export_mock_data("dir")

# and later you can just import it
p.import_mock_data("dir")
```

## Sharing culture - what we can share here?

1. own source snippet - a piece of code that creates our spotify data iterator. ie.

> hey guys I get the spotify data this way, the data is in `data` folder, change the code to get it from somewhere else

```python
files = []
for file in files:

    def fix_timestamps():
        pass

    ## Load JSON document into a dictionary
    with open(os.path.join("data", file), 'r', encoding="utf-8") as f:
        data = json.load(f)

    if file.startswith('Playlist'):
        # this will extract into default schema, you can set any any properties of the table you want
        p.extract(map(fix_timestamps, data['playlists']), table_name='Playlist', write_disposition="replace", columns=[...])
    elif file.startswith('SearchQueries'):
        p.extract(map(fix_timestamps, data), table_name='SearchQueries', write_disposition="replace")
    elif file.startswith('StreamingHistory'):
        p.extract(iter(data), table_name='StreamingHistory', write_disposition="append")
    elif file.startswith('YourLibrary'):
        p.extract(iter(data['tracks']), table_name='YourLibrary', write_disposition="replace")
    else:
        continue
```

2. the whole pipeline could be shared