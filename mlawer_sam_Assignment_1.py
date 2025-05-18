from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

def parse_line(line):
    fields = line.split(',')
    if len(fields) < 17:
            return None  # Skip bad lines

    try:
        fields[4] = float(fields[4])
        fields[16] = float(fields[16])
        return tuple(fields)
    except:
        return None  # Skip bad lines

#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    spark = SparkSession.builder.getOrCreate()

    # input_path = "taxi-data-sorted-small.csv"
    # testDataFrame = spark.read.format('csv').options(header='false', inferSchema='true',  sep =",").load(input_path)
    # rdd = testDataFrame.rdd.map(tuple)
    rdd = sc.textFile(sys.argv[1]).filter(lambda line: line and line.strip() != "")
    rdd = rdd.map(parse_line)
    rdd = rdd.filter(lambda x: x is not None)
    rdd = rdd.filter(correctRows)
    
    # print(rdd.take(1))

    # #Task 1
    top10 = rdd.map(lambda x: (x[0], x[1])).distinct().map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b).takeOrdered(10, key=lambda x: -x[1])
    results_1 = sc.parallelize([f"{medallion},{count}" for medallion, count in top10])
    # print(results_1)

    results_1.coalesce(1).saveAsTextFile(sys.argv[2])


    # #Task 2
    best_drivers = rdd.map(lambda x: (x[1], (x[4] / 60, x[16]))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).mapValues(lambda x: x[1] / x[0]).takeOrdered(10, key=lambda x: -x[1])
    # print(best_drivers)
    results_2 = sc.parallelize([f"{medallion},{moneypermin}" for medallion, moneypermin in best_drivers])

    # #savings output to argument
    results_2.coalesce(1).saveAsTextFile(sys.argv[3])


    # #Task 3 - Optional 
    # #Your code goes here

    # #Task 4 - Optional 
    # #Your code goes here


    sc.stop()