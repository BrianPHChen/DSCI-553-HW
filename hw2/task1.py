from pyspark import SparkContext
import os
import json
import sys
import math
from itertools import combinations
import time

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

case_number = sys.argv[1]
support = int(sys.argv[2])
#input_file_path = "../resource/asnlib/publicdata/small1.csv"
input_file_path = sys.argv[3]
#output_file_path = "./out.txt"
output_file_path = sys.argv[4]

start = time.time()
sc = SparkContext('local[*]', 'task1')
textRDD = sc.textFile(input_file_path)

header = textRDD.first()
if (case_number == "1"):
    baskets = textRDD.filter(lambda x: x != header) \
                    .map(lambda line: (line.split(",")[0], line.split(",")[1])) \
                    .groupByKey() \
                    .map(lambda x: list(set(x[1])))
else:
    baskets = textRDD.filter(lambda x: x != header) \
                    .map(lambda line: (line.split(",")[1], line.split(",")[0])) \
                    .groupByKey() \
                    .map(lambda x: list(set(x[1])))
basketNum = baskets.count()
# partitions = baskets.getNumPartitions()

    
def getC1(dataSet):
    c1 = dict()
    for li in dataSet:
        for item in li:
            if item not in c1:
                c1[item] = 1
            else:
                c1[item] += 1
    return c1
    
def c2l(dataSet, sup):
    l = list()
    for k in dataSet:
        if dataSet[k] >= sup:
            l.append(k)
    return sorted(l)

def l2c(dataSet, prev_l, k):
    c = dict()
    for l in dataSet:
        li = sorted(set(prev_l).intersection(set(l)))
        for item in combinations(li, k):
            if item not in c:
                c[item] = 1
            else:
                c[item] += 1 
    return c

def phase1(iterator):
    chunkBaskets = list(iterator)
    #print("test")
    #print(chunkBaskets)
    localSup = math.ceil((len(chunkBaskets) / basketNum) * support)
    #result = dict()
    candidates = []
    c = getC1(chunkBaskets)
    l = c2l(c, localSup)
    candidates.extend([(item,) for item in l])
    #print("one-tuple")
    #print(candidates)
    k = 2
    while l:
        #print("k: ", k)
        c = l2c(chunkBaskets, l, k)
        l = c2l(c, localSup)
        candidates.extend(l)
        tmp = set()
        for item in l:
            tmp = tmp.union(set(item))
        l = sorted(tmp)
        k += 1
    return candidates

candidates = baskets.mapPartitions(phase1).distinct().sortBy(lambda x: (len(x), x)).collect()

def phase2(dataSet, candidates):
    counts = dict()
    for l in dataSet:
        for item in candidates:
            if set(item).issubset(l):
                if item not in counts:
                    counts[item] = 1
                else:
                    counts[item] += 1
    return [(k, v) for k, v in counts.items()]

# print(candidates)

frequent_itemsets = baskets.mapPartitions(lambda partition: phase2(partition, candidates)) \
                    .reduceByKey(lambda x, y: x+y) \
                    .filter(lambda x: x[1] >= support) \
                    .map(lambda x: x[0]) \
                    .sortBy(lambda x: (len(x), x)) \
                    .collect()

# print(frequent_itemsets)

def covert2Str(data):
    result = ""
    length = 1
    for item in data:
        if (len(item) == 1):
            result += str(item).replace(",", "") + ","
        elif (len(item) == length):
            result += str(item) + ","
        else:
            result += "\n\n"
            result += str(item) + ","
            length += 1
    return result.replace(",\n\n", "\n\n")[:-1]

with open(output_file_path, 'w') as output:
    output.write("Candidates:\n" + covert2Str(candidates) + "\n\n" + "Frequent Itemsets:\n" + covert2Str(frequent_itemsets))

duration = time.time() - start
print("Duration: {}".format(duration))