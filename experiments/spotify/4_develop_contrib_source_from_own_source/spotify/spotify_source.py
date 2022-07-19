import os
from dlt import source as src

# we start with the schema we developed in `own source` workflow.
# we let people pass the directory with the spotify data
@source(schema=src.load_schema("schema.yml"))
def spotify(data_dir):

    # this is the lowest effort, almost all the code is copied verbatim

    files = []
    # check typical places where user data lands
    for dirname in [data_dir, os.path.join(data_dir, 'MyData')]:
        if os.path.isdir(dirname):
            for file in os.listdir(dirname):
                if (file.startswith('Playlist') or
                    file.startswith('SearchQueries') or
                    file.startswith('StreamingHistory') or
                    file.startswith('YourLibrary')):
                    files.append(file)
                else:
                    continue
        # files found good to go
        if len(files) > 0:
            break;

    # instructions if spotify data is not found
    if len(files) == 0:
        raise ValueError(f"""
        "Spotify data could not be found in {data_dir}
        Unzip your Spotify archive and drop the JSON files into `data` directory at the root of the repository
        """)

    ## Iterate through the files
    for file in files:

        ## Load JSON document into a dictionary
        with open(os.path.join(dirname, file), 'r', encoding="utf-8") as f:
            data = json.load(f)

        # example of python data transformation
        def fix_timestamps(item):
            # we realized that every table uses different and sometimes non-standard timestamp
            # representation so dlt autodetection does not always work (probably each table comes from different Spotify department)
            # modify the data on the fly (happens inside the extract function)
            if "lastModifiedDate" in item:
                item["lastModifiedDate"] = str(pendulum.parse(item["lastModifiedDate"]))

            if "searchTime" in item:
                if item["searchTime"].endswith("[UTC]"):
                    item["searchTime"] = item["searchTime"][:-5]
            return item

        if file.startswith('Playlist'):
            # we yield the data as table. we just provide the table name, the rest comes from the schema we loaded earlier
            yield as_table(map(fix_timestamps, data['playlists']), table_name='Playlist')
        elif file.startswith('SearchQueries'):
            yield as_table(map(fix_timestamps, data), table_name='SearchQueries')
        elif file.startswith('StreamingHistory'):
            yield as_table(iter(data), table_name='StreamingHistory')
        elif file.startswith('YourLibrary'):
            yield as_table(iter(data['tracks']), table_name='YourLibrary')
        else:
            continue
