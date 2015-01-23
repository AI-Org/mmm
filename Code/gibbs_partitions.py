# -*- coding: utf-8 -*-
"""
Created on Wed Jan 1 10:13:16 2015

@author: ssoni
"""
#h2_partitions
def partitionByh2(h2):
    return int(str(h2)[0]) % 5

def group_partitionByh2(obj):
    return int(str(obj[2])[0]) % 5    

# here the obj is key h2 h1, we assume this partitioning will be used 
# where keyby is already applied on the data set
def partitionByh2h1(obj):
    n = int(str(obj[2])[0]) % 5
    return getCode(n,str(obj[1]))
    
# Partition data with h2,h1 keys


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
    n = int(str(obj[2])[0]) % 5
    return getCode(n,str(obj[1]))
#

def geth1(h1):
    if len(str(h1)) == 2:
        return int(str(h1)[1])
    else:
        return int(str(h1)[1:3])


def getCode(n,h1):
    if n == 1:
        if h1[0] == "A":
           return 5 * geth1(h1)
        else:
            return 5 * (15 + geth1(h1))
    if n == 2:
        if h1[0] == "A":
           return 1 + 5 * geth1(h1)
        else:
            return 5 * (15 + geth1(h1)) + 1
    if n == 3:
        if h1[0] == "A":
           return 2 + (5 * geth1(h1))
        else:
            return 5 * (15 + geth1(h1)) + 2
    if n == 4:
        if h1[0] == "A":
           return 3 + (5 * geth1(h1))
        else:
            return 5 * (15 + geth1(h1)) + 3
    if n == 0:
        if h1[0] == "A":
           return 4 + (5 * geth1(h1))
        else:
            return 5 * (15 + geth1(h1)) + 4
        