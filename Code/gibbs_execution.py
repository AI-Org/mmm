import sys
import re

from pyspark import SparkContext
import gibbs_init as gi

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
    # First the Gibbs init function
    
    ols_or_random = sys.argv[2] if len(sys.argv) > 2 else "ols" 
    p = sys.argv[3] if len(sys.argv) > 3 else 14  # todo convert sysarhs to int
    gi.gibbs_init_test(sc, d, keyBy_groupby_h2_h1, ols_or_random, p)
    # calling the first UDF of gibbs
    # d_array_agg_sql = gibbs_init.create_d_array_agg_sql()

    #print d_array_agg_sql
    sc.stop()
