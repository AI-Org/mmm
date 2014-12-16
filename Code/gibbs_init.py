import numpy as np
from pyspark import SparkContext
import sys
import gibbs_udf as gu

#'p_var' = Number of explanatory variables in the model, including the intercept term.
p_var = 14
    
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
    return (keys, x_matrix, y_array, hierarchy_level2, hierarchy_level1)   

    
def create_xtx_matrix_xty(obj):
    import numpy
    
    #recObj is of the form of [key, <Iterable of all values tuples>]
    keys = obj[0]
    x_matrix = obj[1] 
    x_matrix_t = numpy.transpose(x_matrix)

    xt_x = x_matrix_t * x_matrix
       
    y_matrix = obj[2]
    xt_y = x_matrix_t * y_matrix
    
    return (keys, xt_x, xt_y)

def get_d_childcount(obj):
        
    keyBy_h2_to_h1 = obj.map(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).groupByKey()
    return keyBy_h2_to_h1.map(lambda (x,iter): (x, sum(1 for _ in set(iter))))

def get_d_count_grpby_level2(obj):
    
    keyBy_h2_week = obj.map(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, week))
    return keyBy_h2_week
    # return keyBy_h2.map(lambda (x,iter): (x, sum(1 for _ in set(iter))))

def get_ols_initialvals_beta_i_j(obj):
    from sklearn import linear_model
    regr = linear_model.LinearRegression()
    regr.fit(obj[1], obj[2])
    print('Coefficients: \n', regr.coef_)
    return (obj[0], regr.coef_)
    
def get_random_initialvals_beta_i(obj):
    coeff = gu.initial_vals_random(p_var)
    #hierarchy_level2 = obj[3]
    #hierarchy_level1 = obj[4]
    return (obj[3], obj[4], coeff)
    
def get_random_initialvals_beta_j(obj):
    coeff = gu.initial_vals_random(p_var)
    #hierarchy_level2 = obj[3]
    #hierarchy_level1 = obj[4]
    return (obj[3], coeff)    

def gibbs_init(model_name, source_RDD, hierarchy_level1, hierarchy_level2, p, df1, y_var, x_var_array, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals):
    text_output = 'Done: Gibbs Sampler for model model_name is initialized.  Proceed to run updates of the sampler by using the gibbs() function.  All objects associated with this model are named with a model_name prefix.'
    return text_output


def gibbs_init_test(sc, d, keyBy_groupby_h2_h1, initial_vals, p):
#if __name__ == '__main__':
    global p_var
    # sc = SparkContext(appName="gibbs_init")
    # of the form keys, x_array, y_array
    m1_d_array_agg = keyBy_groupby_h2_h1.map(create_x_matrix_y_array)
    
    #  we need to make use of X'X and X'y
    m1_d_array_agg_constants = m1_d_array_agg.map(create_xtx_matrix_xty)

    #print m1_d_array_agg_constants.take(1)

    # Compute the childcount at each hierarchy level
    # computing the number of hierarchy_level2 nodes for each of the hierarchy_level1 node
    # think of h2 as department and h1 as the stores 
    # the following computes the number of stores in each department
    key_pairs = get_d_childcount(d)
    print "d_child_counts are : ", key_pairs.collect()
    
    # since the number of weeks of data for each deparment_name-tiers is different.
    # we wll precompute this quantity for each department_name-tier
    m1_d_count_grpby_level2 = get_d_count_grpby_level2(d)
    print "Available data for each department_name-tiers", m1_d_count_grpby_level2.countByKey()

    # structure to compute maps by h2 as key only at m1_d_array_agg levels
    keyBy_h2 = d.keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2)).groupByKey().map(create_x_matrix_y_array)
      
    if(initial_vals == "ols"):
    # Compute OLS estimates for reference
        m1_ols_beta_i = m1_d_array_agg.map(get_ols_initialvals_beta_i_j)
        print "Coefficients for LL after keyby H2", m1_ols_beta_j.collect()
        
        m1_ols_beta_j = keyBy_h2.map(get_ols_initialvals_beta_i_j)
        print "Coefficients for LL after keyby H2", m1_ols_beta_j.collect()
    
    if(initial_vals == "random"):
        print "Draw random array samples of p elements from the uniform(-1,1) dist'n"
        p_var = p

        m1_ols_beta_i = m1_d_array_agg.map(get_random_initialvals_beta_i).groupByKey()
        
        m1_ols_beta_j = keyBy_h2.map(get_random_initialvals_beta_j).groupByKey()
        
    
        
    
        
        
        
        
   
    
   
    
    
