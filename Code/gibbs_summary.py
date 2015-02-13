"""
@author: ssoni

Gibbs summary
Call as  gibbs_summary( text, text, bigint, bigint)
"""
import pickle
import glob
import gibbs_transformations as gtr
import time
import timeit
from pyspark import SparkContext


def m1_summary_geweke_conv_diag_detailed(sc, hdfs_dir, hierarchy_level1, hierarchy_level2, raw_iters, burn_in):
    """
        -- Compute Geweke Convergence Diagnostic (CD) to confirm that the draws of beta_i from the Gibbs Sampler are stationary.  
        -- Break up post burn-in draws from the Gibbs Sampler into 3 pieces.  
        -- 1st piece: First 10% of the draws.
        -- 3rd piece: Last 40% of the draws
        -- 2nd piece: "Middle" draws that are not in the 1st or 3rd piece
        -- The CD is computed by making comparisons between the 1st and 3rd pieces.  If characteristics of the 1st and 3rd piece are similar,
        then there isn't significant evidence that the Sampler has not converged.  
        See Koop pp.66 for more details.

        -- Compute CD and store in a table.  CD is assumed to follow a Standard Normal Distribution.
    """
    # structured as (h2, h1, driver) -> (s, h2, h1, beta_draw[i], x_array[i], h2_h1_driver)
    # reading from local dir of driver
    # read data
    
    # reading from local dir itself.
    path = '/home/ssoni/mmm_t/Code/result/m1_beta_i_draw_*.data'  
    files=glob.glob(path)  
    #m1_dict_10_percent = {}
    #m1_dict_40_percent = {}
    #m1_dict_Mid_50_percent = {}
    m1_dict_10_percent = []
    m1_dict_40_percent = []
    m1_dict_Mid_50_percent = []
    for file in files:     
        f=open(file, 'rb')  
        #f.readlines() 
        obj_dict = pickle.load(f) 
        #print obj_dict.keys()
        k = obj_dict.keys()
        v = obj_dict.values()
        #print k, v
        if k[0] < 0.1 * (raw_iters - burn_in) :
            m1_dict_10_percent = m1_dict_10_percent + v[0]
            #m1_dict_10_percent[k[0]] = v
        if k[0] > 0.6 * (raw_iters - burn_in) :
            #m1_dict_40_percent[k[0]] = v
            m1_dict_40_percent = m1_dict_40_percent + v[0]
        else:
            #m1_dict_Mid_50_percent[k[0]] = v
            m1_dict_Mid_50_percent = m1_dict_Mid_50_percent + v[0]
        
        f.close() 
    #output = open('output.txt', 'rb')
    #obj_dict = pickle.load(output)      
    #m1_beta_i_draw_long = sc.textFile(hdfs_dir+ "m1_beta_i_draw_long*.data")
    # load it as a text file
    print "Summary Started at ", time.strftime("%a, %d %b %Y %H:%M:%S")
    from datetime import datetime
    start_time = datetime.now()
    #m1_beta_i_draw_long = sc.pickleFile(hdfs_dir+ "m1_beta_i_draw_long*.data")
    # 1, [(1, 4, 128, 17126325.852874778, '1', '4-:-128-:-1'), (1, 4, 128, 71313545.095800832, 'x1', '4-:-128-:-x1')]
    m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_percent = sc.parallelize(m1_dict_10_percent).keyBy(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver): (h2, h1, driver))
    #m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_percent = m1_beta_i_draw_long.filter(lambda (s, rows):(s < 0.1 * (raw_iters - burn_in))).values().keyBy(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver): (h2, h1, driver))
    ####.keyBy(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver): (h2, h1, driver)) - no need as its already grouped by h1 h2
    m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_percent = sc.parallelize(m1_dict_40_percent).keyBy(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver): (h2, h1, driver))
    #m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_percent = m1_beta_i_draw_long.filter(lambda (s, rows):(s > 0.6 * (raw_iters - burn_in))).values().keyBy(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver): (h2, h1, driver))
    ####.keyBy(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver): (h2, h1, driver)) - no need as its already grouped by h1 h2
    
    #m1_beta_i_draw_long = sc.newAPIHadoopFile(hdfs_dir+ "m1_beta_i_draw_long*.data", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat","org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.Text")
    #m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_percent = m1_beta_i_draw_long.filter(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver):(s < 0.1 *(raw_iters - burn_in))).keyBy(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver): (h2, h1, driver))
    #m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_percent = m1_beta_i_draw_long.filter(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver):(s > 0.6 * (raw_iters - burn_in))).keyBy(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver): (h2, h1, driver))
    # (h1,h2,driver) - > Iterable of (s, h2, h1, beta_i_draw, driver, h2_h1_driver)
    m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_grp_by_key = m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_percent.groupByKey()
    #print "GROUP by key", m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_grp_by_key.map(lambda (x,y): (x, list(y))).take(30)
    #print "KEYS ", m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_grp_by_key.map(lambda (x,y): (x, list(y))).keys().count() ## 1890 right = h1_h2 distinct pairs
    #print "COUNT BY KEYS ", sorted(m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_grp_by_key.map(lambda (x,y): (x, list(y))).countByKey().items()) 
    geweke_part_10_percent = m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_grp_by_key.map(lambda (x, y): (x, gtr.compute_se_sa_i_avg_sa_i(list(y), raw_iters, burn_in))).keyBy(lambda (x, y): x)
    
    m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_grp_by_key = m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_percent.groupByKey()
    #print "GROUP by key", m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_grp_by_key.map(lambda (x,y): (x, list(y))).take(30)
    #print "KEYS ", m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_grp_by_key.map(lambda (x,y): (x, list(y))).keys().count()
    #print "COUNT BY KEYS ", sorted(m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_grp_by_key.map(lambda (x,y): (x, list(y))).countByKey().items())
    geweke_part_40_percent = m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_grp_by_key.map(lambda (x, y): (x, gtr.compute_se_sc_i_avg_sc_i(list(y), raw_iters, burn_in))).keyBy(lambda (x, y): x)
       
    Joined_geweke_part_10_percent_with_geweke_part_40_percent = geweke_part_10_percent.cogroup(geweke_part_40_percent)
    #print "JOINED ", Joined_geweke_part_10_percent_with_geweke_part_40_percent.map(lambda (x,y): (list(y[0])[0], list(y[1])[0])).take(1)
    # get_cd_beta_i uses se_sa_i, avg_sa_i, se_sc_i, avg_sc_i which is list(y[0])[0][0], list(y[0])[0][1], list(y[1])[0][0], list(y[1])[0][1]
    m1_summary_geweke_conv_diag_detailed = Joined_geweke_part_10_percent_with_geweke_part_40_percent.map(lambda (x,y): (x, list(y[0])[0][1][0], list(y[0])[0][1][1], list(y[1])[0][1][0], list(y[1])[0][1][1], gtr.get_cd_beta_i(list(y[0])[0][1][0], list(y[0])[0][1][1], list(y[1])[0][1][0], list(y[1])[0][1][1])))
    #print "m1_summary_geweke_conv_diag_detailed ", m1_summary_geweke_conv_diag_detailed.take(10)

    end_time = datetime.now()
    print "End of Summary statistics"
    print('Duration: {}'.format(end_time - start_time))
    ## for 100 iterations Finished Computing Geweke Convergence Diagnostic (CD) in time 0.445631027222
    print "Finished Computing Geweke Convergence Diagnostic (CD) in time", str(stop - start)
    # Write via the driver program itself
    l = m1_summary_geweke_conv_diag_detailed.collect()
    output = open("/home/ssoni/mmm_t/Code/result_diag/m1_summary_geweke_conv_diag_detailed.data",'ab+')
    pickle.dump(l, output) 
    output.close()    
    
    return m1_summary_geweke_conv_diag_detailed

# -- Count number of coefficients where the CD falls outside of the 95% interval.  
# #  By chance alone, 5% of the marginal posterior distributions should appear non-stationary when stationarity exists (http://www.bayesian-inference.com/softwaredoc/Geweke.Diagnostic).

def m1_summary_geweke_conv_diag(m1_summary_geweke_conv_diag_detailed):
    cd_signif = m1_summary_geweke_conv_diag_detailed.filter(lambda (x, se_sa_i, avg_sa_i, se_sc_i, avg_sc_i, cd_beta_i): (abs(cd_beta_i)>1.96)).count()  
    print "cd_signif ", cd_signif
    denom = m1_summary_geweke_conv_diag_detailed.count()
    print "denom ", denom
    cd_pct = float(cd_signif)/float(denom)
    
    output = open("/home/ssoni/mmm_t/Code/result_diag/cd_pct.data",'ab+')
    pickle.dump(cd_signif, output)
    pickle.dump(denom, output)
    pickle.dump(cd_pct, output) 
    output.close()
    print "Summary Finished at ", time.strftime("%a, %d %b %Y %H:%M:%S")
    return cd_pct


## run as
##
## spark-submit  --master yarn-client --num-executors 128 --executor-memory 2G --conf spark.executor.extraLibraryPath=/usr/local/lib --conf spark.shuffle.spill=true --conf spark.executor.extraJavaOptions=-XX:+UseCompressedOops   --py-files  gibbs_init.py,gibbs_udfs.py,wishart.py,nearPD.py,gibbs.py,gibbs_transformations.py,gibbs_partitions.py gibbs_summary.py 6    
# Count number of coefficients where the CD falls outside of the 95% interval 0.999332443258
#if __name__ == "__main__":
#    print "hello"
#    sc = SparkContext(appName="GibbsSamplerSummary")
#    hdfs_dir = "hdfs://hdm1.gphd.local:8020/user/ssoni/data/result/" 
#    hierarchy_level2 = 1
#    hierarchy_level1 = 0
#    raw_iters = 100
#    burn_in = 0
#    m1_summary_geweke_conv_diag_detailed = m1_summary_geweke_conv_diag_detailed(sc, hdfs_dir, hierarchy_level1, hierarchy_level2, raw_iters, burn_in)
#    print "m1_summary_geweke_conv_diag_detailed count", m1_summary_geweke_conv_diag_detailed.count()
#    #print "m1_summary_geweke_conv_diag_detailed take 1", m1_summary_geweke_conv_diag_detailed.take(1)
#
#    cd_pct = m1_summary_geweke_conv_diag(m1_summary_geweke_conv_diag_detailed)
#    print "Count number of coefficients where the CD falls outside of the 95% interval", cd_pct  