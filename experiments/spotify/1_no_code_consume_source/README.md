# Consume a ready source without Python code
We can load a source from the command line. Imagine this:

```
pip install python-dlt
pip install python-dlt-contrib

dlt init

vi .dlt/secrets

dlt source info dlt.contrib.sources.spotify

> displays the source docstring and parameters to be passed - all from function signature!

dlt load --to bigquery --dataset spotify dlt.contrib.sources.spotify --data-dir my_data --tables library,history
```

## How does it work

1. `dlt init` created default pipeline folder structure with empty `secrets` and `config` where options are present but commented out. `dlt init` can take a parameter which is a name of the workflow ie. `consume` to consume source, `pipeline` to write a new pipeline, `source` to develop source
2. we put secrets into `.dlt/secrets`
3. we use one amazing python library called fire https://github.com/google/python-fire to generate CLI from the source in `dlt.contrib.sources.spotify`
4. we load the source with `dlt load`
5. we use default set of credentials from `secrets`. if they are not present env variables will be used. OFC there will be `credentials` parameter to pass info on credentials and credentials provider

## dlt load

How does it work
1. we tell the function to load to bigquery into dataset `spotify`
2. we tell it where source module is `dlt.contrib.sources.spotify` - we could figure out for sure how to use an alias ie `sources.spotify` as well
3. fire will figure out the CLI for the module above for the function signature (function must be decorated by @source)
4. we pass the same parameters we would pass in python. in this case this is `data-dir` and `tables`
5. as the source returns iterator or iterators of iterators we know what to do: we'll extract (in parallel if possible), unpack and load atomically and with retries

We could use `.dlt/config`