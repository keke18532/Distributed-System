#!/usr/bin/env python
#
# This script gives an example for Spark assignments on the 
# Distributed systems course. Note that a Spark context depends on 
# specific platform and settings. Please modify this file and play
# with it to get familiar with Spark.
#
# Liang Wang @ CS Dept, Helsinki University, Finland
# 2015.01.19 (modified 2016.11.29, owaltari)
#


import os
import sys
from operator import add
from math import floor
### Dataset
DATA1 = '/cs/work/scratch/spark-data/data-1.txt'


### Some variables you may want to personalize
AppName = "example"
TMPDIR = "/cs/work/scratch/spark-tmp"


### Creat a Spark context on Ukko cluster
from pyspark import SparkConf, SparkContext
conf = (SparkConf()
        .setMaster("spark://ukko007:7077")
        .setAppName(AppName)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true")
        .set("spark.cores.max", 10)  # do not be greedy :-)
        .set("spark.local.dir", TMPDIR))
sc = SparkContext(conf = conf)


### Put your algorithm here.

'''
calculate function is used to 
'''
def calculate(fn):
    data = sc.textFile(fn)
    data = data.map(lambda s: float(s))
    myAvg = data.mean()
    myMin = data.min()
    myMax = data.max()
    myVar = data.variance()
    return [myMin,myMax,myAvg,myVar]
    
def mode(fn):
    data = sc.textFile(fn)
    data = data.map(lambda s: [float(s),1])
    arr = sorted(data.reduceByKey(add).collect())
    return arr[0]
def histo(fn,var):
    data = sc.textFile(fn)
    data = data.map(lambda s: [int(floor(float(s)/var)),1])
    arr = sorted(data.reduceByKey(add).collect())
    for i in range(int(100/var)):
	for j in range(len(arr)):
		if i != arr[i][0]:
			arr.insert(i,(i,0))
    file1 = open("file1.txt","wb")
    for i in arr:
	file1.write(str(i[0])+' '+str(i[1])+'\n')
    file1.close()
    return arr
if __name__=="__main__":
    if sys.argv[1] == "1":
    	myArr = calculate(DATA1)
    	print "Min = %.8f Max = %.8f Avg = %.8f Var = %.8f " % (myArr[0],myArr[1],myArr[2],myArr[3])
    if sys.argv[1] == "2":
    	print mode(DATA1)
    if sys.argv[1] == "3":
    	var=float(sys.argv[2])
    	histo(DATA1,var)
    sys.exit(0)




