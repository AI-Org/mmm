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


#################
import re
### getting m1_d_array_agg out
def parseData(data):
    columns = re.split(",", data)
    return columns 
    #return (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)

def load(source):
    return sc.textFile(source).map(lambda datapoint: parseData(datapoint))

# key[h2,h1] => index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13    
def group_partitionByh2h1(obj):
    n = int(str(obj[2])[1]) % 5
    return getCode1(n,str(obj[1]))
    #return n*100 + h1_int

def geth1(h1):
    if len(str(h1)) == 4:
        return int(str(h1)[2])
    else:
        return int(str(h1)[2:4])

def getCode1(n,h1):
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


# error in docker : string index out of range
# In docker use


def group_partitionByh2h1(obj):
    n = int(str(obj[2])[0]) % 5
    return getCode1(n,str(obj[1]))
#

def geth1(h1):
    if len(str(h1)) == 2:
        return int(str(h1)[1])
    else:
        return int(str(h1)[1:3])


def getCode1(n,h1):
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



#file = "hdfs:///data/d_500.csv"
source = "hdfs:///user/ssoni/data/d.csv"
d = load(source)


def create_x_matrix_y_array(recObj):
    """
       Take an iterable of records, where the key corresponds to a certain age group
       Create a numpy matrix and return the shape of the matrix
       #recObj is of the form of [<all values tuples>]
    """
    import numpy
    recIter = recObj
    keys = recObj[0] # partition value
    recIter = recObj[1]
    mat = numpy.matrix([r for r in recIter])
    x_matrix = mat[:,5:18].astype(float)
    x_matrix = numpy.append([[1 for _ in range(0,len(x_matrix))]], x_matrix.T,0).T
    y_array = mat[:,4].astype(float)
    hierarchy_level2 =  mat[:,2]
    hierarchy_level1 = mat[:,1]
    #return (keys, x_matrix, y_array, hierarchy_level2[1,0], hierarchy_level1[1,0])
    return (keys, x_matrix, y_array, hierarchy_level2[1,0], hierarchy_level1[1,0])


def create_xtx_matrix_xty(obj):
    import numpy
    #recObj is of the form of [key, <Iterable of all values tuples>]
    keys = obj[0] # partition value
    x_matrix = obj[1]
    x_matrix_t = numpy.transpose(x_matrix)
    xt_x = x_matrix_t * x_matrix
    y_matrix = obj[2]
    xt_y = x_matrix_t * y_matrix
    hierarchy_level2 =  obj[3]
    hierarchy_level1 = obj[4]
    # h2, h1, xtx, xty
    return (hierarchy_level2, hierarchy_level1, xt_x, xt_y)


#h1_h2_partitions = 135
h1_h2_partitions = 150
d_groupedBy_h1_h2 = d.groupBy(group_partitionByh2h1, h1_h2_partitions)
m1_d_array_agg = d_groupedBy_h1_h2.map(create_x_matrix_y_array) 
    
m1_d_array_agg_constants = m1_d_array_agg.map(create_xtx_matrix_xty)
    
# AS BROADCAST ONLY m1_d_childcount = d_groupedBy_h1_h2.map(lambda (x,iter): (x, sum(1 for _ in set(iter))), preservesPartitioning=True).cache()
# following goes on the cluster node

def get_d_childcount_mod(obj):
    keyBy_h2_to_h1 = obj.map(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).groupByKey()
    # returns DS with key hierarchy_level2 and value <hierarchy_level2, n1>
    return keyBy_h2_to_h1.map(lambda (x,iter): (int(str(x)[0]), sum(1 for _ in set(iter))))

m1_d_childcount = get_d_childcount_mod(d).collect()
#def group_partitionByh2(obj):
#    return int(str(obj[2])[1]) % 5 
    
# goes on the docker container

def group_partitionByh2(obj):
    return int(str(obj[2])[0]) % 5

h2_partitions = 5

d_groupedBy_h2 = d.groupBy(group_partitionByh2, h2_partitions)
d_keyBy_h2 = d_groupedBy_h2.map(create_x_matrix_y_array)

## do OLS
# beta_i
def get_ols_initialvals_beta_i(obj):
    from sklearn import linear_model
    regr = linear_model.LinearRegression()
    # fit x_array, y_array
    regr.fit(obj[1], obj[2])
    # returns h2, h1, regr.coef_
    return (obj[3], obj[4], regr.coef_)

   

m1_ols_beta_i = m1_d_array_agg.map(get_ols_initialvals_beta_i).keyBy(lambda (h2,h1,coff): (h2))

## beta_j
def get_ols_initialvals_beta_j(obj):
    from sklearn import linear_model
    regr = linear_model.LinearRegression()
    # fit x_array, y_array
    regr.fit(obj[1], obj[2])
    #hierarchy_level2 = a matrix obj[3] of same values in hierarchy_level2
    return (obj[3], regr.coef_)

m1_ols_beta_j = d_keyBy_h2.map(get_ols_initialvals_beta_j).keyBy(lambda (h2,coff): (h2))


def group_partitionByh2(obj):
    return int(str(obj[0])[0]) % 5

# run as random -> it uses functions from transformations and from the udfs
#m1_ols_beta_i = m1_d_array_agg.map(gtr.get_random_initialvals_beta_i, preservesPartitioning=True)
#m1_ols_beta_j = d_keyBy_h2.map(gtr.get_random_initialvals_beta_j, preservesPartitioning=True)


## stuck at m1_ols_beta_i.coalesce(5, shuffle = False).glom().take(2) as it does not honor the conditioned partitioning

def get_Vbeta_i_mu_coeff_i_coeff_j(list_coeff_i, coeff_j):
    #<h2,list<h1,h2,coef_i>,coef_j>
    Vbeta_i_mu_ar = []
    for r in list_coeff_i:
        values_array_i = r[2]    
        Vbeta_i_mu_ar.append(Vbeta_i_mu(values_array_i, coeff_j))
    return Vbeta_i_mu_ar


def get_Vbeta_j_mu(y):
    # now y is an ResultIterable object pointing to a collection of arrays
    # where each array has a structure like <h2,list<h1,h2,coef_i>,coef_j>
    result_Iterable_list = list(y)[0]
    Vbeta_i_mu_ar = get_Vbeta_i_mu_coeff_i_coeff_j(result_Iterable_list[1], result_Iterable_list[2])
    # one can also obtain Vbeta_i_mu_sum as  map(lambda (x,y): (x, sum(fun(list(y)))), joined_i_j_rdd.take(1))
    # corresponding to each one of the h2 level
    Vbeta_i_mu_sum = sum(Vbeta_i_mu_ar)
    Vbeta_j_mu = matrix_add_diag_plr(Vbeta_i_mu_sum, 14)
    # iter, hierarchy_level2, Vbeta_j_mu
    return result_Iterable_list[0],Vbeta_j_mu
    


m1_Vbeta_j_mu = joined_i_j_rdd.map(lambda (x, y): (1, get_Vbeta_j_mu(y)), preservesPartitioning = True).persist() 
m1_Vbeta_j_mu.take(1) 

m1_Vbeta_j_mu.map(lambda (x,y): int(str(y[0])[0])).take(1)  # [h2 in int form] 