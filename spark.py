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
calculate function is used to calculate minimum, maximum, average, and variance for the given data set.
default functions are used here.
'''
def calculate(fn):
    data = sc.textFile(fn)
    data = data.map(lambda s: float(s))
    myAvg = data.mean()
    myMin = data.min()
    myMax = data.max()
    myVar = data.variance()
    return [myMin,myMax,myAvg,myVar]
'''
mode function is used to compute the mode for the data set.
It creates a two-dimensional list out of the original data,
then uses the reduceByKey method to combine the list with same key value.
For example, the two lists [1.5 , 1] and [1.5, 1] are combined into a single list [1.5 , 2].
The it sorts the result and the number which occurs most is ordered to be the first one in the turple.
the result returned is a list which contains the mode and its occurance time.
'''
def mode(fn):
    data = sc.textFile(fn)
    data = data.map(lambda s: [float(s),1])
    arr = sorted(data.reduceByKey(add).collect())
    return arr[0]
'''
histo function takes a parameter 'var' to determine how many lines will be output to the file.
It divides the original data by the given 'var' and leave out the decimal digits. 
Then as before, combines the list with the same key value.
For example, if we want to obtain 1000 lines. The two datas 19.87512229 and 19.89598495 will be converted to two lists
[198,1] and [198,1]. Then they will be combined to be [198,2].
Then there are two for loops, still takes 1000 lines' output as an example, one loop loops for 1000 times, another loop 
loops among the turple and check whether the indexes matches, and if they don't match, a list with value 0 will be inserted. i.e. if the result we obtain is ([1,1],[2,5],[4,3] ... ), since the expected [3,0] is missing, the first loop loops to 3, and the second loop loops to 4, then [3,0] is inserted since the indexes are not equal.
Finally, it outputs the result to the file named 'histo.txt'.
'''
def histo(fn,var):
    data = sc.textFile(fn)
    data = data.map(lambda s: [int(floor(float(s)/var)),1])
    arr = sorted(data.reduceByKey(add).collect())
    for i in range(int(100/var)):
	for j in range(len(arr)):
		if i != arr[i][0]:
			arr.insert(i,(i,0))
    file1 = open("histo.txt","wb")
    for i in arr:
	file1.write(str(i[0])+' '+str(i[1])+'\n')
    file1.close()
    return arr

'''
Here we check if the given parameter is 1 , 2 or 3.
If it is 1, calculate function is called.
If it is 2, mode function is called.
If it is 3, histo function is called, and the second parameter is passed to be the 'var' in the histo function.
'''
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




