from dlt import pipeline as p
from dlt.sources import spotify

# you can configure your pipeline explicitely
# if not configured, sensible defaults will be used: the current file name will become the name of the pipeline
# the working dir goes to tmp and is recreated each time pipeline is run
# this also writes `.dlt/config` file
# dlt.config("spotify_pipeline", work_dir="~/pipelines/spotify")

# this get a list of iterators with associated schema
data = spotify.get_data("/path/to/data", tables=["songs", "history"])
# this will get data from list of iterators and build the schema
p.extract(data)

# this will normalize the json (does unpack if necessary) and load the data into the warehouse
# credentials come from `secrets` file in `_dlt folder` or from environment variables
# see README for alternative loading mechanism
p.load(to="bigquery", credentials=dlt.credentials["bigquery_creds"])
