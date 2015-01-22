# -*- coding: utf-8 -*-
"""
Created on Wed Jan 1 10:13:16 2015

@author: ssoni
"""
#h2_partitions
def partitionByh2(h2):
    return int(str(h2)[1]) % 5

def group_partitionByh2(obj):
    return int(str(obj[2])[1]) % 5    

# here the obj is key h2 h1, we assume this partitioning will be used 
# where keyby is already applied on the data set
def partitionByh2h1(obj):
    n = int(str(obj[2])[1]) % 5
    return getCode(n,str(obj[1]))
    
# Partition data with h2,h1 keys
def geth1(h1):
    if len(str(h1)) == 4:
        return int(str(h1)[2])
    else:
        return int(str(h1)[2:4])

# here object is one whole data point 
# index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13 
# # hierarchy_level2 | n1 
#------------------+----
# 2                | 30
# 1                | 30
# 4                | 30
# 3                | 30
# 5                | 15               
def group_partitionByh2h1(obj):
    n = int(str(obj[2])[1]) % 5
    return getCode(n,str(obj[1]))
    #return n*100 + h1_int

def getCode(n,h1):
    if n == 1:
        if h1[1] == "A":
           return geth1(h1)
        else:
            return 15 + geth1(h1)
    if n == 2:
        if h1[1] == "A":
           return 30 + geth1(h1)
        else:
            return 45 + geth1(h1)
    if n == 3:
        if h1[1] == "A":
           return 60 + geth1(h1)
        else:
            return 75 + geth1(h1)
    if n == 4:
        if h1[1] == "A":
           return 90 + geth1(h1)
        else:
            return 105 + geth1(h1)
    if n == 0:
        if h1[1] == "B":
           return 120 + geth1(h1)
        else:
            return 135 + geth1(h1)
        