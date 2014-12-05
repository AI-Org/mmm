import gibbs_udfs
import sys

from pyspark import SparkContext


def load(source):
    return sc.textFile("file:///d_small.csv")

if __name__ == "__main__":
    """
        Usage: gibbs_execution.py [file]
    """
    sc = SparkContext(appName="GibbsSampler")
    file = sys.argv[1] if len(sys.argv) > 1 else "d_small.csv" 
    d = load(file)

    # calling the first UDF of gibbs
    #d_array_agg_sql = gibbs_udfs.create_d_array_agg_sql()


    sc.stop()
