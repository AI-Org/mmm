import numpy as np
from pyspark import SparkContext
import sys

def create_d_array_agg_sql(d):
    t = d.map()   
    print t
    

def gibbs_init(model_name, source_RDD, hierarchy_level1, hierarchy_level2, p, df1, y_var, x_var_array, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals):
    text_output = 'Done: Gibbs Sampler for model model_name is initialized.  Proceed to run updates of the sampler by using the gibbs() function.  All objects associated with this model are named with a model_name prefix.'
    return text_output


#def gibbs_test():
if __name__ == '__main__':
    
    sc = SparkContext(appName="gibbs_init")

    model_name = "m1"
    file = sys.argv[1] if len(sys.argv) > 1 else "d_small.csv"
    source_RDD = sc.textFile(file).flatMap(lambda x:x.split(','))
    sourceRDD_KEYMAP = source_RDD.map(lambda x: (x(1),x))
    print source_RDD.take(1)
    print sourceRDD_KEYMAP.take(1)
    print np.add(1,2)
    #hierarchy_level1 = sourceRDD.
    #hierarchy_level2=
    #p=
    #df1=
    #y_var=
    #x-var_array=
    #coef_means_prior_array=
    #coef_precision_prior_array=
    #sample_size_deflator=
    #initial_vals=
    #gibbs_init(model_name, source_RDD, hierarchy_level1, hierarchy_level1, p, df1, y_var, x_var_array,coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals)

