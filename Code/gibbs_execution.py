import sys
import re

from pyspark import SparkContext
import gibbs_init as gi
import gibbs

# run as spark-submit --py-files gibbs_init.py,gibbs_udfs.py,wishart.py,nearPD.py gibbs_execution.py

def parseData(data):
    columns = re.split(",", data)
    return columns 
    #return (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)

def load(source):
    return sc.textFile(source).map(lambda datapoint: parseData(datapoint))
    #   return sc.textFile(source)

if __name__ == "__main__":
    """
        Usage: gibbs_execution.py [file]
    """
    sc = SparkContext(appName="GibbsSampler")
    file = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///data/d.csv" 
    d = load(file) ## all data as separate columns
    keyBy_groupby_h2_h1 = d.keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).groupByKey().cache()     
    
    print keyBy_groupby_h2_h1.take(1)
    sample_size_deflator = 1
    # First the Gibbs init function
    
    ols_or_random = sys.argv[2] if len(sys.argv) > 2 else "ols" 
    p = sys.argv[3] if len(sys.argv) > 3 else 14  # todo convert sysarhs to int
    # df1 = defree of freedom 
    df1 = sys.argv[4] if len(sys.argv) > 4 else 15
    # coef_precision_prior_array = Priors for coefficient covariances at the upper-most level of the hierarchy. 
    #coef_precision_prior_array = sys.argv[5] if len(sys.argv) > 5 else [1,1,1,1,1,1,1,1,1,1,1,1,1]
    coef_precision_prior_array = sys.argv[5] if len(sys.argv) > 5 else [1,1,1,1,1,1,1,1,1,1,1,1,1,1]
    #'coef_means_prior_array' = Priors for coefficient means at the upper-most level of the hierarchy. 
    coef_means_prior_array = sys.argv[5] if len(sys.argv) > 5 else [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    gi.gibbs_init_test(sc, d, keyBy_groupby_h2_h1, ols_or_random, p)
    
    # Calling the iterative gibbs algorithm
    hierarchy_level1 = 1
    hierarchy_level2 = 2
    y_var_index = 4
    x_var_indexes = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17] 
    #gibbs.gibbs_test(sc, d, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, begin_iter, end_iter, keyBy_groupby_h2_h1)
    # print d_array_agg_sql
    sc.stop()
