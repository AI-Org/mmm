import numpy as np
from pyspark import SparkContext
import sys


    
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
    return (keys, x_matrix, y_array)   

    
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
      

def gibbs_init(model_name, source_RDD, hierarchy_level1, hierarchy_level2, p, df1, y_var, x_var_array, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals):
    text_output = 'Done: Gibbs Sampler for model model_name is initialized.  Proceed to run updates of the sampler by using the gibbs() function.  All objects associated with this model are named with a model_name prefix.'
    return text_output


def gibbs_init_test(sc, d, keyBy_groupby_h2_h1):
#if __name__ == '__main__':
    
    # sc = SparkContext(appName="gibbs_init")
    
    m1_d_array_agg = keyBy_groupby_h2_h1.map(create_x_matrix_y_array)
    
    #  we need to make use of X'X and X'y
    m1_d_array_agg_constants = m1_d_array_agg.map(create_xtx_matrix_xty)
    
    print m1_d_array_agg_constants.take(1)