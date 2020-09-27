import h5py
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

reader = DataFileReader(open("song.avro", "rb"), DatumReader())


source="ARI6CSW1187B9AF09A"
artist_set=set()

f = open("input_similar", "w")


for song in reader:
    if song["artist_id"] in artist_set:
        continue
    else:
        artist_set.add(song["artist_id"])
    str=","
    line=song["artist_id"]+"\t"+str.join(song["similar_artists"])
    if song["artist_id"] == source:
        line = line + "|" + "0" + "|" + "GRAY\n"
        print(line)
    else:
        line = line + "|" + "Integer.MAX_VALUE" + "|" + "WHITE\n"
    f.write(line)

f.close()
reader.close()
