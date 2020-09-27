import fastavro
import pandas

avro_schema = {"namespace": "example.avro",
 "type": "record",
 "name": "Song",
 "fields": [
     {"name": "file",  "type": "string"},
     {"name": "year", "type": "int"},
     {"name": "hot",  "type": "double"},
     {"name": "duration",  "type": "double"},
     {"name": "energy",  "type": "double"},
     {"name": "tempo",  "type": "double"},
     {"name": "release",  "type": "string"},
     {"name": "release_7digitalid",  "type": "long"},
     {"name": "track_7digitalid",  "type": "long"},
     {"name": "track_id",  "type": "string"},
     {"name": "song_id",  "type": "string"},
     {"name": "title",  "type": "string"},
     {"name": "artist_7digitalid",  "type": "long"},
     {"name": "artist_id",  "type": "string"},
     {"name": "artist_name", "type": "string"},
     {"name": "similar_artists", "type": {"type":"array", "items":"string"}}
 ]
}

avro_schema = fastavro.parse_schema(avro_schema)
avro_file = "song.avro"
parquet_file = "song.parquet"


def avro_df(filepath, encoding):
    with open(filepath, encoding) as fp:
        reader = fastavro.reader(fp)
        records = [r for r in reader]
        df = pandas.DataFrame.from_records(records)
        return df

df = avro_df(avro_file, 'rb')
df.to_parquet(parquet_file)
