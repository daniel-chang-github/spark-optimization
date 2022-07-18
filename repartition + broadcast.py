'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month,broadcast

import os
import time

spark = SparkSession.builder.appName('Optimize I').getOrCreate()

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3]) 

answers_input_path = os.path.join(project_path, 'Desktop/Data_Engineering/Spark Optimization/Optimization/data/answers')

questions_input_path = os.path.join(project_path, 'Desktop/Data_Engineering/Spark Optimization/Optimization/data/questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()

'''
Answers aggregation

Here we : get number of answers per question per month
'''

start_time = time.time()

spark.conf.set("spark.sql.adaptive.enabled","true")

answers_month = answersDF.withColumn('month', month('creation_date'))
answers_month = answers_month.repartition(col("month"))
answers_month = answers_month.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = questionsDF.join(broadcast(answers_month), "question_id").select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF.orderBy('question_id', 'month').show()

print("Processing time: %s seconds" % (time.time() - start_time))
print("\n")
resultDF.explain()


'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''