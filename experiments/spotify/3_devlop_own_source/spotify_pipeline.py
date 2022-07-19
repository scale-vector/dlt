from dlt import pipeline as p

# could be replaced by `.dlt/config`
# data will load into "spotifypipeline", schemas will be synced via folder "schemas", tables will be re-created with each run
# also pipeline will be wiped out each time it is run, until working_dir is set
# to those are good defaults for initial experimentation
p.config(sync_schema_folder="schemas", recreate_tables=True)

# here the exact code from our demo goes. `extract` however takes additional parameters to define key schema properties explicitely

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

# now we have a spotify source, let's say we want to add some related annotations from google sheets. this happens via
# source you get from contrib
# this you add to the pipeline once your custom source is fully developed - see README

# put annotation in separate schema. annotations data will load into `spotifypipeline_annotations`
# we also limit nesting of the schema
ann_schema = Schema("annotations", max_nesting=2)
from contrib.sources import prodigy
p.extract(prodigy.get_data("spotify"), schema=ann_schema)

# process the data but do not actually load to big query so credentials are not requires
p.load(to="bigquery", dry_run=True)

# this will actually load your data
# p.load(to="bigquery", credentials=dlt.credentials["my_creds"])

# at this moment we have `schema.yml` in schemas folder. if you change it, it will be passed to the pipeline on the next run
