# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 00:26:01 2020

@author: zehua
"""
import os
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


spark = SparkSession.builder \
    .master('local') \
    .appName('myAppName') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()


sc = spark.sparkContext
sqlContext = SQLContext(sc)
df = sqlContext.read.parquet('song.parquet')

dfnew = df.select(df.artist_id,df.similar_artists)
dfnew=dfnew.drop_duplicates(['artist_id'])
rdd = dfnew.rdd

counter = sc.accumulator(0)

targetA = "ARI6CSW1187B9AF09A"

def toNode(data):
    target = data[0]
    connections = data[1]
    searchStatus = 'White'
    distance = 10000
    if (target == targetA):
        searchStatus = 'Gray'
        distance = 0
    return (target, (connections, distance, searchStatus))



newrdd = rdd.map(toNode)


def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]


    results = []

    if color == 'Gray':
        for connection in connections:
            newCharacterID = connection
            newDistance = distance+1
            newColor = 'Gray'
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)
        counter.add(1)
        color = 'Black'
    
    results.append((characterID, (connections, distance, color)))
    return results


def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    
    distance1 = data1[1]
    distance2 = data2[1]
    
    color1 = data1[2]
    color2 = data2[2]
    
    edges = []
    distance = 9999
    color = 'White'
    
    if len(edges1) > 0:
        edges = edges1
    elif len(edges2) > 0:
        edges = edges2
        
    if distance1 < distance:
        distance = distance1
    if distance2 < distance:
        distance = distance2
        
    if color1 == 'White':
        color = color2
    elif color1 == 'Gray':
        if color2 == 'Black':
            color=color2
        else:
            color=color1
    else:
        color=color1

    return (edges, distance, color)
    



prev=0
for iteration in range(22):

    counter.value=0
    print('Running BFS iteration: ' + str(iteration+1))
    mapped = newrdd.flatMap(bfsMap)

    # mapped.collect()

    print('Processing ' + str(mapped.count()) + ' values.')
    print(counter.value-prev)
    prev=counter.value-prev

        
    newrdd = mapped.reduceByKey(bfsReduce)

    if prev==0:
        f = open("pyspark_result/"+str(iteration+1), "w")
        list_elements = newrdd.collect()
        for element in list_elements:
          f.write(str(element))
          f.write("\n")
        f.close()
        break;





