# -*- coding: utf-8 -*-
"""
Created on Tue Jan 20 10:03:02 2015

@author: ssoni
"""
###
import sys
import re

def geth2(data):
    columns = re.split(",", data)[1]
    return columns 

def load_key_h2(source):
    return sc.textFile(source).map(lambda datapoint: geth2(datapoint)).keyBy(lambda (hierarchy_level2): (hierarchy_level2))

d_h2 = load_key_h2(file)
h2_partitions = d_h2.groupByKey().keys().count()

## Example of hash partitioning using only h2 as the key
## here our 1. partition has all the data pertaining to that key
## as such we can use functions like mappartitions which are applied on each partitions specifically and nothing is lost
#
#import sys
#import re

def partitionByh2(hierarchy_level2):
    int(str(hierarchy_level2)[1]) % h2_partitions
    
def parseData(data):
    columns = re.split(",", data)[0:6]
    return columns 
    # for computing the hash function we have
    # index, hierarchy_level1, hierarchy_level2, week, y1, x1
    #return (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)

def load_data_partitioned_with_h2(source):
    #return sc.textFile(source).map(lambda datapoint: parseData(datapoint)).partitionBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1): hierarchy_level2).persist()
    return sc.textFile(source).map(lambda datapoint: parseData(datapoint)).keyBy(lambda (index, hierarchy_level1, h2, week, y1, x1): h2).partitionBy(5, lambda (hierarchy_level2): int(str(hierarchy_level2)[1]) % 5).persist()


file = "hdfs:///data/d_500.csv"
    ## load all data as separate columns
import time

start = time.time()
d_key_partitioned_h2 = load_data_partitioned_with_h2(file)

end = time.time()
print end - start

### >>> len(d.glom().take(5))      
### 5
### >>> len(d.glom().take(6))
### 5
### >>> len(d.glom().take(7))
### 5

###################################################################################################

import sys
import re

def geth1h2(data):
    columns = re.split(",", data)[1:3]
    return columns 

def load_h1_h2(source):
    #return sc.textFile(source).map(lambda datapoint: parseData(datapoint)).partitionBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1): hierarchy_level2).persist()
    return sc.textFile(source).map(lambda datapoint: geth1h2(datapoint)).keyBy(lambda (hierarchy_level1, h2): (h2, hierarchy_level1))

d_h1_h2 = load_h1_h2(file)
h1_h2_partitions = d_h1_h2.groupByKey().keys().count()
## d_h1_h2.groupByKey().keys().count() : 135                                                
##PythonRDD[108] at RDD at PythonRDD.scala:43
##>>> d_h1_h2.groupByKey().keys().collect()
##[(u'"1"', u'"B8"'), (u'"4"', u'"A2"'), (u'"4"', u'"A4"'), (u'"3"', u'"A7"'), (u'"3"', u'"B14"'), (u'"4"', u'"A12"'), (u'"2"', u'"B11"'), (u'"2"', u'"B13"'), (u'"3"', u'"B2"'), (u'"3"', u'"B8"'), (u'"3"', u'"A11"'), (u'"4"', u'"B1"'), (u'"4"', u'"B15"'), (u'"2"', u'"B9"'), (u'"2"', u'"A14"'), (u'"1"', u'"A7"'), (u'"4"', u'"A10"'), (u'"1"', u'"B14"'), (u'"3"', u'"B10"'), (u'"2"', u'"A12"'), (u'"5"', u'"B2"'), (u'"1"', u'"B10"'), (u'"1"', u'"B6"'), (u'"1"', u'"A13"'), (u'"4"', u'"B11"'), (u'"2"', u'"A4"'), (u'"2"', u'"B15"'), (u'"3"', u'"A1"'), (u'"1"', u'"B4"'), (u'"1"', u'"A9"'), (u'"1"', u'"A1"'), (u'"2"', u'"A6"'), (u'"4"', u'"B5"'), (u'"3"', u'"A5"'), (u'"3"', u'"A15"'), (u'"5"', u'"B10"'), (u'"3"', u'"A13"'), (u'"5"', u'"B12"'), (u'"3"', u'"B12"'), (u'"2"', u'"A10"'), (u'"3"', u'"B4"'), (u'"4"', u'"A14"'), (u'"5"', u'"B8"'), (u'"4"', u'"B7"'), (u'"4"', u'"B3"'), (u'"2"', u'"A2"'), (u'"4"', u'"A6"'), (u'"4"', u'"A8"'), (u'"1"', u'"B12"'), (u'"1"', u'"A3"'), (u'"1"', u'"A15"'), (u'"2"', u'"B1"'), (u'"5"', u'"B6"'), (u'"1"', u'"A5"'), (u'"5"', u'"B4"'), (u'"4"', u'"B13"'), (u'"3"', u'"A9"'), (u'"3"', u'"B6"'), (u'"1"', u'"A11"'), (u'"2"', u'"A8"'), (u'"5"', u'"B14"'), (u'"2"', u'"B7"'), (u'"2"', u'"B5"'), (u'"3"', u'"A3"'), (u'"1"', u'"B2"'), (u'"2"', u'"B3"'), (u'"4"', u'"B9"'), (u'"2"', u'"A5"'), (u'"1"', u'"A12"'), (u'"3"', u'"A8"'), (u'"3"', u'"A6"'), (u'"4"', u'"B8"'), (u'"1"', u'"A10"'), (u'"1"', u'"A4"'), (u'"1"', u'"A2"'), (u'"2"', u'"A3"'), (u'"4"', u'"B12"'), (u'"2"', u'"B4"'), (u'"3"', u'"B1"'), (u'"2"', u'"A9"'), (u'"4"', u'"A13"'), (u'"4"', u'"A15"'), (u'"4"', u'"A5"'), (u'"1"', u'"B15"'), (u'"3"', u'"B9"'), (u'"3"', u'"A2"'), (u'"1"', u'"B5"'), (u'"2"', u'"B2"'), (u'"2"', u'"B10"'), (u'"4"', u'"A1"'), (u'"5"', u'"B1"'), (u'"1"', u'"A6"'), (u'"1"', u'"B1"'), (u'"2"', u'"A15"'), (u'"2"', u'"A7"'), (u'"4"', u'"A3"'), (u'"4"', u'"B4"'), (u'"5"', u'"B5"'), (u'"3"', u'"A10"'), (u'"1"', u'"B9"'), (u'"2"', u'"B8"'), (u'"1"', u'"A14"'), (u'"1"', u'"B13"'), (u'"3"', u'"B3"'), (u'"3"', u'"B15"'), (u'"4"', u'"A11"'), (u'"2"', u'"B6"'), (u'"4"', u'"B2"'), (u'"2"', u'"A1"'), (u'"5"', u'"B3"'), (u'"3"', u'"B7"'), (u'"1"', u'"B3"'), (u'"5"', u'"B15"'), (u'"4"', u'"B14"'), (u'"4"', u'"A7"'), (u'"3"', u'"A12"'), (u'"5"', u'"B7"'), (u'"1"', u'"B11"'), (u'"5"', u'"B13"'), (u'"1"', u'"A8"'), (u'"2"', u'"A11"'), (u'"3"', u'"A14"'), (u'"2"', u'"B12"'), (u'"3"', u'"B11"'), (u'"3"', u'"A4"'), (u'"2"', u'"A13"'), (u'"4"', u'"B6"'), (u'"4"', u'"A9"'), (u'"3"', u'"B13"'), (u'"5"', u'"B9"'), (u'"1"', u'"B7"'), (u'"3"', u'"B5"'), (u'"2"', u'"B14"'), (u'"5"', u'"B11"'), (u'"4"', u'"B10"')]
###################################################################################################

def parseData(data):
    columns = re.split(",", data)
    return columns 
    # for computing the hash function we have
    # index, hierarchy_level1, hierarchy_level2, week, y1, x1
    #return (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)


    
def geth1(h1):
    if len(str(h1)) == 4:
        return int(str(h1)[2])
    else:
        return int(str(h1)[2:4])
#    int(str(hierarchy_level2)[1]) % 5

def partitionByh2h1(obj): 
    h1_int = geth1(obj[1])
    n = int(str(obj[0])[1]) % 5
    return n*100 + h1_int

def load_h2(source):
    #return sc.textFile(source).map(lambda datapoint: parseData(datapoint)).partitionBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1): hierarchy_level2).persist()
    return sc.textFile(source).map(lambda datapoint: parseData(datapoint)).keyBy(lambda (index, hierarchy_level1, h2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (h2, hierarchy_level1)).partitionBy(135, partitionByh2h1).persist()


file = "hdfs:///data/d.csv"
    ## load all data as separate columns
import time

d = load_h2(file)

end = time.time()
print end - start

## d.glom().take(1)

keyBy_groupby_h2_h1 = d.keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1): (hierarchy_level2, hierarchy_level1)).groupByKey().cache()     
print "Cached Copy of Data, First Data Set : ", keyBy_groupby_h2_h1.take(1)
