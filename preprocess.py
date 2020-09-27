
import h5py
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

import os
all_files=[]
for first in range(ord('A'),ord('B')+1):
    dir_first=os.listdir(str(chr(first)))
    for second in dir_first:
        if ord(second[0]) in range(ord('A'),ord('Z')+1):
            second_s=str(chr(first))+"/"+second
            dir_second=os.listdir(second_s)
            for third in dir_second:
                if ord(third[0]) in range(ord('A'),ord('Z')+1):
                    prefix=second_s+"/"+third
                    files=os.listdir(prefix)
                    print(prefix)
                    for file in files:
                        if os.path.splitext(file)[-1] == '.h5':
                            all_files.append(prefix+"/"+file)


schema = avro.schema.parse(open("song.avsc", "rb").read())
writer = DataFileWriter(open("song.avro", "wb"), DatumWriter(), schema)

for filename in all_files:
    print(filename)

    f = h5py.File(filename,'r')

    info={}
    encoding = 'utf-8'

    info['file']=filename
    info["year"]=f["musicbrainz"]["songs"]["year"][0].item()
    info["hot"]=f["metadata"]["songs"]["song_hotttnesss"][0].item()
    info["duration"]=f["analysis"]["songs"]["duration"][0].item()
    info["energy"]=f["analysis"]["songs"]["energy"][0].item()
    info["tempo"]=f["analysis"]["songs"]["tempo"][0].item()
    info["release"]=f["metadata"]["songs"]["release"][0].decode(encoding)
    info["release_7digitalid"]=f["metadata"]["songs"]["release_7digitalid"][0].item()
    info["track_7digitalid"]=f["metadata"]["songs"]["track_7digitalid"][0].item()
    info["track_id"]=f["analysis"]["songs"]["track_id"][0].decode(encoding)
    info["song_id"]=f["metadata"]["songs"]["song_id"][0].decode(encoding)
    info["title"]=f["metadata"]["songs"]["title"][0].decode(encoding)
    info["artist_7digitalid"]=f["metadata"]["songs"]["artist_7digitalid"][0].item()
    info["artist_id"]=f["metadata"]["songs"]["artist_id"][0].decode(encoding)
    info["artist_name"]=f["metadata"]["songs"]["artist_name"][0].decode(encoding)
    tmp=f["metadata"]["similar_artists"][:]
    similar_artists=[]
    for artist in tmp:
        similar_artists.append(artist.decode(encoding))
    info['similar_artists']=similar_artists

    f.close()

    writer.append({"file":filename, "year": info["year"], "hot": info["hot"], "duration": info["duration"],
                   "energy": info["energy"], "tempo": info["tempo"],"release": info["release"],
                   "release_7digitalid": info["release_7digitalid"], "track_7digitalid": info["track_7digitalid"],
                   "track_id": info["track_id"], "song_id": info["song_id"], "title": info["title"],
                   "artist_7digitalid": info["artist_7digitalid"], "artist_id": info["artist_id"],
                   "artist_name": info["artist_name"], "similar_artists": info["similar_artists"]})

writer.close()


