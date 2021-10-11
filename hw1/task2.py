from pyspark import SparkContext
import os
import json
import sys
import time

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

#test_review_path = "../resource/asnlib/publicdata/test_review.json"
test_review_path = sys.argv[1]
#output_path = "./output2.json"
output_path = sys.argv[2]
input_n_partition = sys.argv[3]
sc = SparkContext('local[*]', 'task2')
textRDD = sc.textFile(test_review_path)
result = dict()

time_start_1 = time.time()

reviewsRDD_1 = textRDD.map(lambda row: json.loads(row))
#F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
businesses = \
reviewsRDD_1.map(lambda review: (review["business_id"], 1)).reduceByKey(lambda a, b: a+b).sortBy(lambda x: (-x[1], x[0])).take(10)

top10_businesses = []
for business in businesses:
    top10_businesses.append([business[0], business[1]])

num_partition = reviewsRDD_1.getNumPartitions()
num_items = reviewsRDD_1.glom().map(len).collect()

time_end_1 = time.time()

result["default"] = dict()
result["default"]["n_partition"] = num_partition
result["default"]["n_items"] = num_items
result["default"]["exe_time"] = time_end_1 - time_start_1

time_start_2 = time.time()

reviewsRDD_2 = textRDD.map(lambda row: json.loads(row))
#F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
reviewsRDD_2 = \
reviewsRDD_2.map(lambda review: (review["business_id"], 1)).partitionBy(int(input_n_partition), lambda x: ord(x[0]) + ord(x[1]))

num_partition = reviewsRDD_2.getNumPartitions()
num_items = reviewsRDD_2.glom().map(len).collect()
    
businesses = reviewsRDD_2.reduceByKey(lambda a, b: a+b).sortBy(lambda x: (-x[1], x[0])).take(10)

top10_businesses = []
for business in businesses:
    top10_businesses.append([business[0], business[1]])

time_end_2 = time.time()

result["customized"] = dict()
result["customized"]["n_partition"] = num_partition
result["customized"]["n_items"] = num_items
result["customized"]["exe_time"] = time_end_2 - time_start_2
"""
for k, v in result["customized"].items():
    print(k, v)
"""
with open(output_path, 'w') as output:
    json.dump(result, output)
