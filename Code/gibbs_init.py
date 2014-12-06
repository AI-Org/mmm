import numpy as np
from pyspark import SparkContext

def create_d_array_agg_sql(d):
    #t = d.map()    


def gibbs_init(model_name, source_RDD, hierarchy_level1, hierarchy_level1, p, df1, y_var, x_var_array,coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals):
    
    
    text_output = 'Done: Gibbs Sampler for model model_name is initialized.  Proceed to run updates of the sampler by using the gibbs() function.  All objects associated with this model are named with a model_name prefix.'
    return text_output



sc = SparkContext(appName="gibbs_init")

model_name="m1"
source_RDD=sc.textFile(sys.argv[1]).flatMap(lambda x:x.split(',')).map(lambda x: ())
hierarchy_level1=
hierarchy_level2=
p=
df1=
y_var=
x-var_array=
coef_means_prior_array=
coef_precision_prior_array=
sample_size_deflator=
initial_vals=
gibbs_init(model_name, source_RDD, hierarchy_level1, hierarchy_level1, p, df1, y_var, x_var_array,coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals)

