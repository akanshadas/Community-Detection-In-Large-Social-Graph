from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import json
import csv
import itertools
import time
import math
import random
import os
from graphframes import *

# variables
filter_threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
output_file_path = sys.argv[3]
m = 5


SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[3]', 'task1')
sc.setLogLevel("ERROR")
ss = SparkSession(sc)

start = time.time()
smallRDD = sc.textFile(input_file_path).map(lambda e: e.split(",")).map(lambda e: (e[0], e[1]))
headers = smallRDD.take(1)
finalRDD = smallRDD.filter(lambda e: e[0] != headers[0][0])

# Step 1 : Creating graph
user_business_list = finalRDD.groupByKey().mapValues(lambda entry: list(set(entry))).collect()
pairs = set()
for u1, b1s in user_business_list:
    for u2, b2s in user_business_list:
        if u1 != u2 and (u1, u2) not in pairs:
            if len(set(b1s) & set(b2s)) >= filter_threshold:
                pairs.add((u1, u2))


nodes = set([user for lp in pairs for user in lp])

edges = sc.parallelize(pairs).map(lambda e: (e[0], e[1], 'sim')).toDF(['src', 'dst', 'relationship'])

un = [tuple([u], ) for u in nodes]
vertices = ss.createDataFrame(un, ['id'])

# graph
graph = GraphFrame(vertices, edges)

user_comms = graph.labelPropagation(maxIter = m)
rz = user_comms.rdd.map(lambda x: (x['label'], x['id'])).groupByKey().map(lambda x: (sorted(list(x[1])), len(x[1]))).sortBy(lambda x: (x[1], x[0])).map(lambda x: x[0])

res = rz.collect()
f = open(output_file_path, "w")
for i in range(len(res)):
    line = ""
    for item in res[i]:
        line += "\'" + str(item) + "\'" + ", "
    if (i != len(res) - 1):
        f.write(line[:-2] + "\n")
    else:
        f.write(line[:-2])
f.close()

end = time.time()
print("\n\nDuration:", end - start)