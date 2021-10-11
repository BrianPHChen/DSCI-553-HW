from pyspark import SparkContext
import os
import json
import sys
import time

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

test_review_path = sys.argv[1]
business_path = sys.argv[2]
output_qa_path = sys.argv[3]
output_qb_path = sys.argv[4]

sc = SparkContext('local[*]', 'task3')

time_start_2 = time.time()

reviewsRDD = sc.textFile(test_review_path).map(lambda row: json.loads(row))
businessRDD = sc.textFile(business_path).map(lambda row: json.loads(row))

#A. What is the average stars for each city?
reviews = reviewsRDD.map(lambda review: (review["business_id"], review["stars"])) \
.join(businessRDD.map(lambda business: (business["business_id"], business["city"]))) \
.filter(lambda review: review[1][1] != None) \
.map(lambda review: (review[1][1], (review[1][0], 1))) \
.reduceByKey(lambda x, y: (x[0]+y[0] ,x[1]+y[1])) \
.mapValues(lambda val: val[0] / val[1]) \
.sortBy(lambda x: (-x[1], x[0])) \

top_10_city1 = reviews.take(10)

time_end_2 = time.time()
"""
for city in top_10_city1:
    print(city)
"""    
with open(output_qa_path, "w") as file:
    file.write("city,stars\r\n")
    for review in reviews.collect():
        file.write(str(review[0]) + "," + str(review[1]) + "\r\n")
        #print(review)

ansB = dict()
        
time_start_1 = time.time()
business2stars = dict()
business2city = dict()
with open(test_review_path) as file:
    for line in file:
        business_id = json.loads(line)["business_id"]
        stars = json.loads(line)["stars"]
        if business_id not in business2stars :
            business2stars[business_id] = [stars]
        else :
            business2stars[business_id].append(stars)

with open(business_path) as file:
    for line in file:
        business_id = json.loads(line)["business_id"]
        business2city[business_id] = json.loads(line)["city"]

result = dict()
for business in business2stars:
    city = business2city[business]
    if city not in result :
        result[city] = business2stars[business]
    else :
        result[city].extend(business2stars[business])

for key, val in result.items():
    avg = sum(val) / len(val)
    result[key] = avg

sorted_result = sorted(result.items(), key=lambda x: (-x[1], x[0]))    

top_10_city2 = sorted_result[:10]

time_end_1 = time.time()

ansB["m1"] = time_end_1 - time_start_1
ansB["m2"] = time_end_2 - time_start_2
ansB["reason"] = "m2 is faster than m1, I thought it's because spark can do the map/reduce to enhance performance. Howerver, m2 is not faster much than m1, it may because the data is too small in test set."
"""
for r in top_10_city2:
    print(r)
"""
#print(ansB)
with open(output_qb_path, 'w') as file:
    json.dump(ansB, file)
        