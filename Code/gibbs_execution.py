import sys

from pyspark import SparkContext


def load(source):
    return sc.textFile(source).keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level1, hierarchy_level2))
#   return sc.textFile(source)

if __name__ == "__main__":
    """
        Usage: gibbs_execution.py [file]
    """
    sc = SparkContext(appName="GibbsSampler")
    file = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///data/d_small.csv" 
    d = load(file)
    print d.take(1)
    # First the Gibbs init function
    # calling the first UDF of gibbs
    # d_array_agg_sql = gibbs_init.create_d_array_agg_sql()

    #print d_array_agg_sql
    sc.stop()
