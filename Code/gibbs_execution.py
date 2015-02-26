import sys
import re

from pyspark import SparkContext
import gibbs_init as gi
import gibbs_summary as gis
import gibbs_partitions as gp
from pyspark.storagelevel import StorageLevel
import gibbs_transformations as gtr
import glob
import pickle

# Following the bug that states as of 1/19/2015: "Calling cache() after RDDs are pipelined has no effect in PySpark"
## https://issues.apache.org/jira/browse/SPARK-3105
# Note that cache() works properly if we call it before performing any other transformations on the RDD:
# At this time, for pyspark, its only statically decided whether to cache() the pipelined RDD and not dynamically decided whether to cache when we perform actions

# Importance has been given to laying out the data structures in most optimal way,
# So as to minimize the network traffic in order to improve performance

def load_previous_values(sc, previous_iter_value, hdfs_dir):
    m1_beta_i_draw = sc.pickleFile(hdfs_dir+"m1_beta_i_draw_"+previous_iter_value+".data")
    m1_beta_i_mean = sc.pickleFile(hdfs_dir+"m1_beta_i_mean_"+previous_iter_value+".data")
    m1_beta_mu_j = sc.pickleFile(hdfs_dir+"m1_beta_mu_j_"+previous_iter_value+".data")
    m1_beta_mu_j_draw = sc.pickleFile(hdfs_dir+"m1_beta_mu_j_draw_"+previous_iter_value+".data")
    m1_h_draw = sc.pickleFile(hdfs_dir+"m1_h_draw_"+previous_iter_value+".data")
    m1_Vbeta_i = sc.pickleFile(hdfs_dir+"m1_Vbeta_i_"+previous_iter_value+".data")
    m1_Vbeta_inv_Sigmabeta_j_draw = sc.pickleFile(hdfs_dir+"m1_Vbeta_inv_Sigmabeta_j_draw_"+previous_iter_value+".data")
    m1_Vbeta_j_mu = sc.pickleFile(hdfs_dir+"m1_Vbeta_j_mu_"+previous_iter_value+".data")
    m1_Vbeta_inv_Sigmabeta_j_draw_h_draws = m1_Vbeta_inv_Sigmabeta_j_draw.keyBy(lambda (sequence, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2).join(m1_h_draw).map(lambda (h2, y): (y[0][0], h2, y[0][2], y[0][3], y[0][4], y[1][2]))
        
    m1_Vbeta_inv_Sigmabeta_j_draw_collection = m1_Vbeta_inv_Sigmabeta_j_draw_h_draws.map(lambda (sequence, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j, h_draw): (hierarchy_level2, Vbeta_inv_j_draw, sequence, n1,  Sigmabeta_j, h_draw)).collect()
    # (1,<objc>)(2,<objc>)...
    #m1_Vbeta_inv_Sigmabeta_j_draw_collection = sorted(map(lambda (sequence, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): (int(str(hierarchy_level2)[0]), Vbeta_inv_j_draw.all(), sequence, n1,  Sigmabeta_j.all()), m1_Vbeta_inv_Sigmabeta_j_draw_collection))
    m1_Vbeta_inv_Sigmabeta_j_draw_collection = sorted(m1_Vbeta_inv_Sigmabeta_j_draw_collection)
    
    return (m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_h_draw ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_inv_Sigmabeta_j_draw_collection, m1_Vbeta_j_mu)
    
def get_constants(d):
    d_groupedBy_h1_h2 = d.keyBy(lambda (hierarchy_level1_h2_key, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): hierarchy_level1_h2_key).groupByKey()
    m1_d_array_agg = d_groupedBy_h1_h2.map(gtr.create_x_matrix_y_array, preservesPartitioning = True).persist(StorageLevel.MEMORY_ONLY_2)
    
    #  Compute constants of X'X and X'y for computing 
    #  m1_Vbeta_i & beta_i_mean
    #  m1_d_array_agg_constants : list of tuples of (h2, h1, xtx, xty)
    # OPTIMIZATION preserving the values on partitions
    m1_d_array_agg_constants = m1_d_array_agg.map(gtr.create_xtx_matrix_xty, preservesPartitioning = True).persist(StorageLevel.MEMORY_ONLY_2)
    # print "m1_d_array_agg_constants take ",m1_d_array_agg_constants.take(1)
    # print "m1_d_array_agg_constants count",m1_d_array_agg_constants.count()
    
    # Compute the childcount at each hierarchy level
    # Compute the number of children at the brand level (# of children is equal to the number of genders per brand), 
    # Compute the number of children at the brand-gender level (# of children is equal to the number of departments per brand-gender), and 
    # Compute the number of children at the department_name level (# of children is equal to the number tiers within each department_name).
    # In Short the following Computs the number of hierarchy_level1 values for each of the hierarchy_level2 values
    # for Example : Considering h2 as departments and h1 as the stores get_d_childcount computes the number of stores for each department
    # OPTIMIZATION using broadcast variable instead of the RDD so as to not compute it ever again
    # this saves us one more scan of the table everytime we compute the childrens of key h2
    # [(u'1', 30), (u'3', 30), (u'5', 30), (u'2', 30), (u'4', 30)]
    # after sorted and new mod function we have [(1, 30), (2, 30), (3, 30), (4, 30), (5, 30)]    
    #OPTIMIZATION  boradcasting it like m1_d_count_grpby_level2_b [(0, 30), (1, 30), (2, 30), (3, 30), (4, 30)]  
    m1_d_childcount_b = sc.broadcast(sorted(gtr.get_d_childcount(d).collect()))
    #print "m1_d_childcount_b ", m1_d_childcount_b.value[0]
    #print "m1_d_childcount_b ", m1_d_childcount_b.value[1]
    #print "m1_d_childcount_b ", m1_d_childcount_b.value[2]
    #print "m1_d_childcount_b ", m1_d_childcount_b.value[3]
    #print "m1_d_childcount_b ", m1_d_childcount_b.value[4]
    #m1_d_childcount_b = sc.broadcast(m1_d_childcount.collect())
    #m1_d_childcount = d_groupedBy_h1_h2.map(lambda (x,iter): (x, sum(1 for _ in set(iter))), preservesPartitioning=True).cache()
    # print "d_child_counts take : ", m1_d_childcount.take(1)
    # print "d_child_counts count : ", m1_d_childcount.count()
     
    # Not all department_name-tiers have the same number of weeks of available data (i.e. the number of data points for each department_name-tier is not the same for all department_name-tiers).  
    # We pre-compute this quantity for each department_name-tier
    # m1_d_count_grpby_level2 = gtr.get_d_count_grpby_level2(d).cache()
    # print "m1_d_count_grpby_level2 take : ", m1_d_count_grpby_level2.take(1)
    # print "m1_d_count_grpby_level2 count : ", m1_d_count_grpby_level2.count()
    # print "Available data for each department_name-tiers", m1_d_count_grpby_level2.countByKey()
    # m1_d_count_grpby_level2.countByKey() becomes defaultdict of type int As
    # defaultdict(<type 'int'>, {u'"5"': 1569, u'"1"': 3143, u'"2"': 3150, u'"3"': 3150, u'"4"': 3150})
    #m1_d_count_grpby_level2 = gtr.get_d_count_grpby_level2(d).countByKey()
    # for multinode setup we need to broadcast these values across all the nodes
    # KEYS OPTIMIZATION defaultdict(<type 'int'>, {0: 3022, 1: 3143, 2: 3150, 3: 3150, 4: 3150})
    m1_d_count_grpby_level2_b = sc.broadcast(gtr.get_d_count_grpby_level2(d).countByKey())
    return (m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount_b, m1_d_count_grpby_level2_b)


## NEW FILE
## NEW FILE
## NEW FILE
## NEW FILE
# new data doesnt have an index column 
# its like (hierarchy_level1_h2_key, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)
# d.map(lambda data: re.split(",",data)).map(lambda (hierarchy_level1_h2_key, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level1_h2_key, hierarchy_level2))
def parseData_newData(data):
    rows = re.split(",", data)
    # h2 keys replaced T4 -> 5 T3 -> 4 T2 -> 3, T1 -> 2, F1 -> 1
    rows[1] = int(str(rows[1])[1]) % 5
    # h1_h2 keys directly converting the strings into ints
    rows[0] = gp.getCode_new(rows[1],int(str(rows[0])))
    # now return (hierarchy_level1_h2_key, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)
    return rows[0:17] 
    #previous return (hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)

def load_new(source):
    return sc.textFile(source).map(lambda datapoint: parseData_newData(datapoint)).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    

## get the number of partitions desired for h2 keyword
def geth2_new(data):
    columns = re.split(",", data)[1]
    return columns

def load_key_h2_new(source):
    return sc.textFile(source).map(lambda datapoint: geth2_new(datapoint)).keyBy(lambda (hierarchy_level2): (hierarchy_level2))    

## NEW FILE
def geth1h2_new(data):
    rows = re.split(",", data)[0:2]
    #h1 = rows[0]
    #h2 = rows[1]
    #r = [geth1(h1), int(str(h2)[1])]
    return rows

def load_key_h1_h2_new(source):
     return sc.textFile(source).map(lambda datapoint: geth1h2_new(datapoint)).keyBy(lambda (hierarchy_level1, hierarchy_level2): (hierarchy_level2, hierarchy_level1))


#def partitionByh2h1(obj):
#    h1_int = geth1(obj[1])
#    n = int(str(obj[0])[1]) % 5
#    return n*100 + h1_int    

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
    hdfs_dir = "hdfs://hdm1.gphd.local:8020/user/ssoni/data/result/" 
    
    # compute partions from Hierarchy levels    
    h2_partitions = load_key_h2_new(sourcefile).groupByKey().keys().count()
    h1_h2_partitions = load_key_h1_h2_new(sourcefile).groupByKey().keys().count()
    print "new H2 partitions ", h2_partitions
    print "new H1 h2 partitions ", h1_h2_partitions
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
    d = load_new(sourcefile) 
    
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
    #    d.saveAsSequenceFile(hdfs_dir+"sequence_api.data")
    #    sc.parallelize([1, 2, 'spark', 'rdd']).saveAsSequenceFile(hdfs_dir+"msequence_api.data")
    #except:
    #    print "Count not write using sequenceFile also"   
    
    # hierarchy_level1 = tier, 
    # hierarchy_level2 = brand_department_number
    previous_iter_value = 0 
    try:
        #previous_iter_value = sc.pickleFile(hdfs_dir+"previous_iter*").collect()[-1]
        path = '/home/ssoni/mmm_t/Code/result/previous_iter*.data'  
        files=glob.glob(path)  
        #for file in files:     
        f=open(files[-1], 'rb')  
        #f.readlines() 
        previous_iter_value = pickle.load(f) 
        f.lcose()
    except:
        print "First iteration assumed. No previous runs founds."
        
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

    if previous_iter_value == 0:
    # First initialize the gibbs sampler
    #(m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw , m1_ols_beta_i ,m1_ols_beta_j ,m1_s2 ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_j_mu) = gi.gibbs_initializer(sc, d, h1_h2_partitions, h2_partitions, d_key_h2, d_key_h2_h1, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals)
        (m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw , m1_ols_beta_i ,m1_ols_beta_j ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_inv_Sigmabeta_j_draw_collection, m1_Vbeta_j_mu) = gi.gibbs_initializer(sc, d, h1_h2_partitions, h2_partitions, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals)
        
    
    begin_iter = sys.argv[13] if len(sys.argv) > 13 else 2
    end_iter = sys.argv[14] if len(sys.argv) > 14 else 100    
    
    if previous_iter_value > 0:  
        (m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount, m1_d_count_grpby_level2) = get_constants(d)
        begin_iter = previous_iter_value + 1
        (m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_h_draw ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_inv_Sigmabeta_j_draw_collection, m1_Vbeta_j_mu) = load_previous_values(sc, str(previous_iter_value), hdfs_dir)
        
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
