import sys
import re

from pyspark import SparkContext
import gibbs_init as gi
import gibbs
import gibbs_summary as gis
import gibbs_partitions as gp
from pyspark.storagelevel import StorageLevel

# Following the bug that states as of 1/19/2015: "Calling cache() after RDDs are pipelined has no effect in PySpark"
## https://issues.apache.org/jira/browse/SPARK-3105
# Note that cache() works properly if we call it before performing any other transformations on the RDD:
# At this time, for pyspark, its only statically decided whether to cache() the pipelined RDD and not dynamically decided whether to cache when we perform actions

# Importance has been given to laying out the data structures in most optimal way,
# So as to minimize the network traffic in order to improve performance

# parse the entire dataset into tuples of values,
# where each tuple is of the type (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)
def parseData(data):
    rows = re.split(",", data)
    # h2 keys
    rows[2] = int(str(rows[2])[0]) % 5
    # h1_h2 keys
    rows[1] = gp.getCode(rows[2],str(rows[1]))
    # now return (hierarchy_level1_h2_key, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)
    return rows[1:18] 
    #previous return (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)

def load(source):
    return sc.textFile(source).map(lambda datapoint: parseData(datapoint)).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    
## get the number of partitions desired for h2 keyword
def geth2(data):
    columns = re.split(",", data)[1]
    return columns

def load_key_h2(source):
    return sc.textFile(source).map(lambda datapoint: geth2(datapoint)).keyBy(lambda (hierarchy_level2): (hierarchy_level2))
    
## get the number of partitions desired for h2,h1 keyword
def geth1h2(data):
    rows = re.split(",", data)[1:3]
    #h1 = rows[0]
    #h2 = rows[1]
    #r = [geth1(h1), int(str(h2)[1])]
    return rows

def load_key_h1_h2(source):
     return sc.textFile(source).map(lambda datapoint: geth1h2(datapoint)).keyBy(lambda (hierarchy_level1, hierarchy_level2): (hierarchy_level2, hierarchy_level1))

# Partition data with h2 keys
def partitionByh2(hierarchy_level2):
    int(str(hierarchy_level2)[1]) % 5
     
# Partition data with h2,h1 keys
def geth1(h1):
    if len(str(h1)) == 4:
        return int(str(h1)[2])
    else:
        return int(str(h1)[2:4])

#def partitionByh2h1(obj):
#    h1_int = geth1(obj[1])
#    n = int(str(obj[0])[1]) % 5
#    return n*100 + h1_int    

## get persisted datastores that work on same partitions.
def get_persisted_by_h2(source, numPartitions):
    return sc.textFile(source).map(lambda datapoint: parseData(datapoint)).keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2)).partitionBy(numPartitions, gp.partitionByh2).persist()

def get_persisted_by_h2_h1(source, numPartitions): 
    return sc.textFile(source).map(lambda datapoint: parseData(datapoint)).keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).partitionBy(numPartitions, gp.partitionByh2h1).persist()



if __name__ == "__main__":
    """
        Usage: gibbs_execution.py [file]
        args:
            'storage_level' : 0 for MEMORY_ONLY, 1 for MEMORY_AND_DISK, 2 for MEMORY_ONLY_SER
                             3 for MEMORY_AND_DISK_SER, 4 for DISK_ONLY, 5 for MEMORY_ONLY_2, 6 MEMORY_AND_DISK_2
                             7 : StorageLevel.MEMORY_AND_DISK_SER_2  & 8 : OFF_HEAP
            'file'          : hdfs:///data/d.csv { Name of source table, arranged in a form where each variable is represented as a separate column }
            'hierarchy_level1' : Name of the bottom-most level of the hierarchy. ( tier )
            'hierarchy_level2' : Name of the upper-most level of the hierarchy.  The "pooling level" of the hierarchy. ( brand_department_number )
            'p'           : Number of explanatory variables in the model, including the intercept term. Defaults to 14.
            'df1'         : Degrees of freedom for Wishart distribution, used when sampling the Level2 covariance matrix.  A reasonable off-the-shelf value is to set 'df1' to p+1, i.e. 15
            'y_var'       : Index of the response variable in the csv file. ( blended_units_total_log. )
            'x_var_array' : Indexes of the explanatory variables provided within an array expression. 
                          ,[1  
                          , num_colors_log_new         
                          , total_sellable_qty_log              
                          , floorset_bts_ind              
                          , holiday_thanksgiving    
                          , holiday_xmas            
                          , holiday_easter       
                          , baseprice_minus_promo_regular_discount_log 
                          , discount_redline_total_log  
                          , store_promo_pct_new  
                          , item_promo_pct_new 
                          , redline_pct 
                          , redline_item_promo_pct 
                          , holiday_pre_xmas]
            'coef_means_prior_array' : Priors for coefficient means at the upper-most level of the hierarchy.  
                          See next line on how to make the prior negligible (i.e. how to specify a noninformative prior under this framework).
            'coef_precision_prior_array' = Priors for coefficient covariances at the upper-most level of the hierarchy.  
                          If no prior information would want to be incorporated, the user would specify arbitrary values for 'coef_means_prior_array' and 
                          specify a set of very small numbers for 'coef_precision_prior_array', such as .000001.
            'sample_size_deflator' = Decreases the confidence that the user has about the data, applicable in situations where the data is known to be not perfect.  
                          Effectively decreases the precision of the data on the final estimated coefficients.  
                          For example, specifying a value of 10 for 'sample_size_deflator' would very roughly decrease the size and 
                          precision of the sample perceived by the Gibbs Sampler by a factor of 10.  
         run : spark-submit --py-files  gibbs_init.py,gibbs_udfs.py,wishart.py,nearPD.py,gibbs.py,gibbs_transformations.py,gibbs_summary.py,gibbs_partition.py gibbs_execution.py
         run as yarn-client :
         spark-submit  --master yarn-client --driver-memory 1g --executor-memory 4g --executor-cores 1 
                       --py-files gibbs_init.py,gibbs_udfs.py,wishart.py,nearPD.py,gibbs.py,gibbs_transformations.py,gibbs_summary.py 
                       --conf spark.shuffle.spill=false --conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops"  gibbs_execution.py
                       Does not run with 4 g of driver-memory and 2 or more executor cores.
        spark-submit  --master yarn-client --py-files gibbs_init.py,gibbs_udfs.py,wishart.py,nearPD.py,gibbs.py,gibbs_transformations.py,gibbs_summary.py,gibbs_partitions.py --conf spark.shuffle.spill=false --conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops"  gibbs_execution.py
                       
    """
    sc = SparkContext(appName="GibbsSampler")
    # defaults to 6 memory and disk storage with duplication
    storageLevel = sys.argv[1] if len(sys.argv) > 1 else 6
    #sourcefile = sys.argv[2] if len(sys.argv) > 2 else "hdfs://sandbox:9000/user/ssoni/data/d.csv"  
    #hdfs_dir = "hdfs:///user/ssoni/data/" 
    sourcefile = sys.argv[2] if len(sys.argv) > 2 else "hdfs://hdm1.gphd.local:8020/user/ssoni/data/d.csv"
    hdfs_dir = "hdfs://hdm1.gphd.local:8020/user/ssoni/data/result" 
    h2_partitions = load_key_h2(sourcefile).groupByKey().keys().count()
    h1_h2_partitions = load_key_h1_h2(sourcefile).groupByKey().keys().count()
    # get all the keys by load_key_h1_h2(sourcefile).groupByKey().keys().sortByKey().collect()
    # hierarchy_level2 | n1 
    #------------------+----
    # 2                | 30
    # 1                | 30
    # 4                | 30
    # 3                | 30
    # 5                | 15
    
    # OPTIMIZATION 1 : replace d with d_key_h2
    #d_key_h2 = get_persisted_by_h2(sourcefile, h2_partitions)
    #d_key_h2_h1 = get_persisted_by_h2(sourcefile, h1_h2_partitions)
        
    ## load all data as separate columns
    d = load(sourcefile) 
    
    #try:
    #    d.saveAsHadoopFile(hdfs_dir+"old_api.data","org.apache.hadoop.mapred.SequenceFileOutputFormat", "org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.Text")
    #
    #    sc.parallelize([1, 2, 'spark', 'rdd']).saveAsHadoopFile(hdfs_dir+"mold_api.data","org.apache.hadoop.mapred.SequenceFileOutputFormat", "org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.Text")
    #except:
    #    print "Count not write using old API file"
    #try:
    #    d.saveAsNewAPIHadoopFile(hdfs_dir+"new_api.data","org.apache.hadoop.mapred.SequenceFileOutputFormat", "org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.Text")
    #    sc.parallelize([1, 2, 'spark', 'rdd']).saveAsNewAPIHadoopFile(hdfs_dir+"mnew_api.data","org.apache.hadoop.mapred.SequenceFileOutputFormat", "org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.Text")
    #except:
    #    print "Count not write using new API file"
    #try:
    #    d.saveAsPickleFile(hdfs_dir+"pickle_api.data", 3)
    #    sc.parallelize([1, 2, 'spark', 'rdd']).saveAsPickleFile(hdfs_dir+"mpickle_api.data", 3)
    #except:
    #    print "Count not write using pickle file too"
    #try:
    #    d.saveAsPickleFile(hdfs_dir+"sequence_api.data")
    #    sc.parallelize([1, 2, 'spark', 'rdd']).saveAsPickleFile(hdfs_dir+"msequence_api.data")
    #except:
    #    print "Count not write using sequenceFile also"   
        
        
    # OPTIMIZATION 2 keyBy_groupby_h2_h1 is essentially d_key_h2_h1 so we use d_key_h2_h1 in place of keyBy_groupby_h2_h1
    # keyBy_groupby_h2_h1 = d.keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).groupByKey().cache()     
    #print "Cached Copy of Data, First Data Set : ", d_key_h2_h1.take(1)
    
    # hierarchy_level1 = tier, 
    # hierarchy_level2 = brand_department_number
    hierarchy_level1 = sys.argv[3] if len(sys.argv) > 3 else 1
    hierarchy_level2 = sys.argv[4] if len(sys.argv) > 4 else 2
    
    # 'p' = Number of explanatory variables in the model, including the intercept term.
    p = sys.argv[5] if len(sys.argv) > 5 else 14  # todo convert sysarhs to int
    # df1 = defree of freedom 
    df1 = sys.argv[6] if len(sys.argv) > 6 else 15    
    # the response variable
    y_var_index = sys.argv[7] if len(sys.argv) > 7 else 4
    # the explanatory variables
    x_var_indexes = sys.argv[8] if len(sys.argv) > 8 else [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
    
    # coef_means_prior_array' = Priors for coefficient means at the upper-most level of the hierarchy. 
    # coef_means_prior_array=sys.argv[5] if len(sys.argv) > 5 else [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    coef_means_prior_array = sys.argv[9] if len(sys.argv) > 9 else [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    
    # coef_precision_prior_array = Priors for coefficient covariances at the upper-most level of the hierarchy. 
    # coef_precision_prior_array=sys.argv[5] if len(sys.argv) > 5 else [1,1,1,1,1,1,1,1,1,1,1,1,1]
    coef_precision_prior_array = sys.argv[10] if len(sys.argv) > 10 else [1,1,1,1,1,1,1,1,1,1,1,1,1,1]
    
    sample_size_deflator = sys.argv[11] if len(sys.argv) > 11 else 1
    
    initial_vals = sys.argv[12] if len(sys.argv) > 12 else "ols" 

    # First initialize the gibbs sampler
    #(m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw , m1_ols_beta_i ,m1_ols_beta_j ,m1_s2 ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_j_mu) = gi.gibbs_initializer(sc, d, h1_h2_partitions, h2_partitions, d_key_h2, d_key_h2_h1, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals)
    (m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw , m1_ols_beta_i ,m1_ols_beta_j ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_inv_Sigmabeta_j_draw_collection, m1_Vbeta_j_mu) = gi.gibbs_initializer(sc, d, h1_h2_partitions, h2_partitions, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals)
        
    
    begin_iter = sys.argv[13] if len(sys.argv) > 13 else 2
    end_iter = sys.argv[14] if len(sys.argv) > 14 else 100    
    
    # Calling the iterative gibbs algorithm 
    (m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_j_mu) = gibbs.gibbs_iter(sc,storageLevel, hdfs_dir, begin_iter, end_iter, coef_precision_prior_array, h2_partitions, m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_inv_Sigmabeta_j_draw_collection, m1_Vbeta_j_mu)
    
    raw_iters = sys.argv[15] if len(sys.argv) > 15 else 100
    
    burn_in = sys.argv[16] if len(sys.argv) > 16 else 0     
    
    # call gibbs summary functions
    m1_summary_geweke_conv_diag_detailed = gis.m1_summary_geweke_conv_diag_detailed(sc, hdfs_dir, hierarchy_level1, hierarchy_level2, raw_iters, burn_in)
    print "m1_summary_geweke_conv_diag_detailed count", m1_summary_geweke_conv_diag_detailed.count()
    print "m1_summary_geweke_conv_diag_detailed take 1", m1_summary_geweke_conv_diag_detailed.take(1)

    cd_pct = gis.m1_summary_geweke_conv_diag(m1_summary_geweke_conv_diag_detailed)
    print "Count number of coefficients where the CD falls outside of the 95% interval", cd_pct    
    
    print "Done: Gibbs Sampler draws have been summarized.  All objects associated with this model are named with a m1 prefix."
    sc.stop()
