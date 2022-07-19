from dlt import pipeline as p

# we import the source we develp
from spotify import spotify

#
# data will load into "spotifydevpipeline", schemas will be synced via folder "schemas", tables will be re-created with each run
# also pipeline will be wiped out each time it is run, until working_dir is set
p.config(sync_schema_folder="schemas", recreate_tables=True)

# consume the source we develop
p.extract(spotify("data"))

# process the data but do not actually load to big query so credentials are not requires
p.load(to="bigquery", dry_run=True)

# this will actually load your data
# p.load(to="bigquery", credentials=dlt.credentials["my_creds"])

