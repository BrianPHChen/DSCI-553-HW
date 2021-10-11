from pyspark import SparkContext
import os
import json
import sys

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

#test_review_path = "../resource/asnlib/publicdata/test_review.json"
test_review_path = sys.argv[1]
#output_path = "./output.json"
output_path = sys.argv[2]
sc = SparkContext('local[*]', 'task1')
textRDD = sc.textFile(test_review_path)
result = dict()

reviewsRDD = textRDD.map(lambda row: json.loads(row))
# A. The total number of reviews
result["n_review"] = reviewsRDD.count()

#B. The number of reviews in 2018
result["n_review_2018"] = \
reviewsRDD.map(lambda review: review["date"][:4]).filter(lambda year: year == "2018").count()

#C. The number of distinct users who wrote reviews
result["n_user"] = \
reviewsRDD.map(lambda review: review["user_id"]).distinct().count()

#D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
users = \
reviewsRDD.map(lambda review: (review["user_id"], 1)).reduceByKey(lambda a, b: a+b).sortBy(lambda x: (-x[1], x[0])).take(10)

top10_users = []
for user in users:
    top10_users.append([user[0], user[1]])

result["top10_user"] = top10_users

#E. The number of distinct businesses that have been reviewed
result["n_business"] = \
reviewsRDD.map(lambda review: review["business_id"]).distinct().count()

#F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
businesses = \
reviewsRDD.map(lambda review: (review["business_id"], 1)).reduceByKey(lambda a, b: a+b).sortBy(lambda x: (-x[1], x[0])).take(10)

top10_businesses = []
for business in businesses:
    top10_businesses.append([business[0], business[1]])

result["top10_business"] = top10_businesses
"""
for k, v in result.items():
    print(k, v)
"""
with open(output_path, 'w') as output:
    json.dump(result, output)
