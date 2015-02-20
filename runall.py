import sys
import re

from pyspark import SparkContext
import gibbs_init as gi
import gibbs_udfs as gu

def parseData(data):
    columns = re.split(",", data)
    return columns
    
    #return (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)

def load(source):
    return sc.textFile(source).map(lambda datapoint: parseData(datapoint))
    #   return sc.textFile(source)
    

file = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///data/d.csv" 
## all data as separate columns
d = load(file) 

#keyby h2 and h1
keyBy_groupby_h2_h1 = d.keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).groupByKey().cache()  

# key by only h2 and h1 again
keyBy_h2_to_h1 = d.map(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2,hierarchy_level1)).groupByKey()
m1_d_childcount = keyBy_h2_to_h1.map(lambda (x,iter): (x, sum(1 for _ in set(iter)))).collect()



def create_x_matrix_y_array(recObj):
    """
       Take an iterable of records, where the key corresponds to a certain age group
       Create a numpy matrix and return the shape of the matrix
    """
    import numpy
    
    #recObj is of the form of [key, <Iterable of all values tuples>]
    keys = recObj[0]
    recIter = recObj[1]    
    
    mat = numpy.matrix([r for r in recIter])
    x_matrix = mat[:,5:18].astype(float)
    y_array = mat[:,4].astype(float)
    hierarchy_level2 =  mat[:,2]
    hierarchy_level1 = mat[:,1]
    return (keys, x_matrix, y_array, hierarchy_level2[1,0], hierarchy_level1[1,0])


# structure to compute maps by h2 as key only at m1_d_array_agg levels
keyBy_h2 = d.keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2)).groupByKey().map(create_x_matrix_y_array)


m1_d_array_agg = keyBy_groupby_h2_h1.map(create_x_matrix_y_array)

def get_ols_initialvals_beta_i(obj):
    from sklearn import linear_model
    regr = linear_model.LinearRegression()
    # fit x_array, y_array
    regr.fit(obj[1], obj[2])
    print('Coefficients: \n', regr.coef_)
    return (obj[3], obj[4], regr.coef_)


def get_ols_initialvals_beta_j(obj):
    from sklearn import linear_model
    regr = linear_model.LinearRegression()
    # fit x_array, y_array
    regr.fit(obj[1], obj[2])
    print('Coefficients: \n', regr.coef_)
    #hierarchy_level2 = a matrix obj[3] of same values in hierarchy_level2
    return (obj[3], regr.coef_)

          
def create_join_by_h2_only(ti,tj):
    # t1 is a list of ((u'"1"', u'"B8"'), (u'"1"', u'"B8"', array([[  7.16677290e-01,   4.15236265e-01,   7.02316511e-02,
    # t2 is a list of (u'"5"', (u'"5"', array([[ 0.86596322,  0.29811589,  0.29083844
    joined = []
    for rec1 in ti:
        keys = rec1[0]
        hierarchy_level2 = keys[0]
        hierarchy_level1 = keys[1]
        values_array_i = rec1[1][2]
        for rec2 in tj:
            hierarchy_level2_rec2 = rec2[0]
            values_array_j = rec2[1][1]
            if(hierarchy_level2 == hierarchy_level2_rec2):
                tup = (hierarchy_level2, hierarchy_level1, values_array_i, values_array_j)
                joined.append(tup)
    return joined

p_var = 14  
def get_Vbeta_j_mu(obj):
    global accum, p_var;
    accum += 1
    keys = obj[0] # hierarchy_level2
    # now Obj1 is an ResultIterable object pointing to a collection of arrays
    # where each array has a structure like <h2,h1,coef_i,coef_j>
    result_list = list(obj[1])  
    Vbeta_i_mu_ar = []
    for r in result_list: 
        values_array_i = r[2]
        values_array_j = r[3]
        Vbeta_i_mu_ar.append(Vbeta_i_mu(values_array_i, values_array_j))       
    Vbeta_i_mu_sum = sum(Vbeta_i_mu_ar)   
    Vbeta_j_mu = matrix_add_diag_plr(Vbeta_i_mu_sum, p_var)
    # iter, hierarchy_level2, Vbeta_j_mu
    return accum, keys, Vbeta_j_mu 

df1_var = 15
# test of positive definate matrix
## m1_Vbeta_j_mu.map(get_m1_Vbeta_j_mu_pinv).collect() returns [True, False, True, True, True]
def get_m1_Vbeta_j_mu_pinv(obj):
    import numpy as np
    global df1_var
    seq = obj[0]
    hierarchy_level2 = obj[1]
    Vbeta_j_mu = obj[2]
    # Vbeta_inv_draw(nu, phi) where nu is df1_var & for phi matrix we have
    phi = np.linalg.pinv(matrix_scalarmult_plr(Vbeta_j_mu, df1_var))  
    ## here
    if np.all(np.linalg.eigvals(phi) > 0) != True:
        phi = nearPD(phi)
        
    return np.all(np.linalg.eigvals(phi) > 0)    
    
    
    Vbeta_inv_j_draw = Vbeta_inv_draw(df1_var, phi)
    return (seq, hierarchy_level2, Vbeta_inv_j_draw)

    
def np_pinv(Vbeta_inv_j_draw, n1, coef_precision_prior_array):
    import numpy as np
    temp = gu.matrix_scalarmult_plr(Vbeta_inv_j_draw, n1)
    temp_add = gu.matrix_scalarmult_plr(temp, gu.matrix_diag_plr(coef_precision_prior_array))
    return np.linalg.pinv(temp_add)
            

# count 135
m1_ols_beta_i = m1_d_array_agg.map(get_ols_initialvals_beta_i).keyBy(lambda (h2,h1,coff): (h2, h1))
print "Coefficients for LL after keyby H2", m1_ols_beta_i.take(1) #135

# count 5  
m1_ols_beta_j = keyBy_h2.map(get_ols_initialvals_beta_j).keyBy(lambda (h2,coff): h2)
print "Coefficients for LL after keyby H2", m1_ols_beta_j.take(1) # 5

accum = sc.accumulator(0) 

# A list of all joined tuples with count 135
# [elems] where one elem of elems looks like (hierarchy_level2, hierarchy_level1, values_array_i, values_array_j)
joined_i_j = create_join_by_h2_only(m1_ols_beta_i.collect(), m1_ols_beta_j.collect())
# keyBy and groupBy will reduce the rows from 135 to 5 since there are only 5 hierarchy_level2's
joined_i_j_rdd = sc.parallelize(joined_i_j).keyBy(lambda (hierarchy_level2, hierarchy_level1, values_array_i, values_array_j): (hierarchy_level2)).groupByKey()
# joined_i_j_rdd.take(1) :  (u'"5"', <pyspark.resultiterable.ResultIterable object at 0x117be50>) similarly 5 others
## checked correct one
m1_Vbeta_j_mu = joined_i_j_rdd.map(get_Vbeta_j_mu) 

print " m1_Vbeta_j_mu ", m1_Vbeta_j_mu.count() # the actual values are 500 I am getting 135 values
print " m1_Vbeta_j_mu ", m1_Vbeta_j_mu.take(1)
    
    
"""
    Errorsome on "matrix is not positive definite." raised by 
    File "gibbs_init.py", line 133, in get_m1_Vbeta_j_mu_pinv
    Vbeta_inv_j_draw = gu.Vbeta_inv_draw(df1_var, phi)
    File "gibbs_udfs.py", line 84, in Vbeta_inv_draw
    return wishartrand(nu, phi)
    m1_Vbeta_j_mu_pinv = m1_Vbeta_j_mu.map(get_m1_Vbeta_j_mu_pinv).keyBy(lambda (seq, hierarchy_level2, Vbeta_inv_j_draw) : (hierarchy_level2)).groupByKey()
    """
    """ 7 more DS after that """
 error in m1_Vbeta_j_mu.map(get_m1_Vbeta_j_mu_pinv) saying matrix is not positive definite
 





    