# -*- coding: utf-8 -*-
"""
@author: ssoni
"""

import gibbs_udfs as gu
import gibbs_transformations as gtr
from pyspark.storagelevel import StorageLevel
import gibbs_partitions as gp
import timeit
import time
def gibbs_iteration_text():
    text_output = 'Done: All requested Gibbs Sampler updates are complete.  All objects associated with this model are named with a m1 prefix.'   
    return text_output


def add(x,y):
    return (x+y)

#                  d, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, begin_iter, end_iter
def gibbs_iter(sc, sl, hdfs_dir, begin_iter, end_iter, coef_precision_prior_array, h2_partitions, m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_inv_Sigmabeta_j_draw_collection, m1_Vbeta_j_mu):
    
    m1_d_array_agg_key_by_h2_h1 = m1_d_array_agg.keyBy(lambda (h2_h1_key, hierarchy_level2, x_matrix, y_array) : (hierarchy_level2, h2_h1_key))
    # OPTIMIZED into grpby_level2    
    m1_d_count_grpby_level2_b = m1_d_count_grpby_level2
    storagelevel = StorageLevel.MEMORY_ONLY
    if sl == 1 :
        storagelevel = StorageLevel.MEMORY_AND_DISK
    if sl == 2 :
        storagelevel = StorageLevel.MEMORY_ONLY_SER
    if sl == 3 :
        storagelevel = StorageLevel.MEMORY_AND_DISK_SER
    if sl == 4 :
        storagelevel = StorageLevel.DISK_ONLY
    if sl == 5 :
        storagelevel = StorageLevel.MEMORY_ONLY_2
    # Best so far is MEMORY_AND_DISK_2
    if sl == 6 :
        storagelevel = StorageLevel.MEMORY_AND_DISK_2
    if sl == 7 :
        storagelevel = StorageLevel.MEMORY_AND_DISK_SER_2
    if sl == 8 :
        storagelevel = StorageLevel.OFF_HEAP
    
    print "Gibbs Iteration starts"
     
    # m1_d_array_agg_constants_key_by_h2_h1 is a large Data Structure which can be persisted across multiple iterations of Gibbs Algorithm
    # Data tuples which will be joined/cogrouped with this data set will be pickled and transferred to each of these nodes carrying 135 partitions.
    ##m1_d_array_agg_constants_key_by_h2_h1 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2, h1)).partitionBy(150).persist(storagelevel)
    ##m1_d_array_agg_constants_key_by_h2 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2)).partitionBy(5).persist(storagelevel)
    # We dont need to partition m1_d_childcount_groupBy_h2 as it is a small data structure which can be shipped to each node already having the persisted data.
    ##m1_d_childcount_groupBy_h2 = m1_d_childcount.keyBy(lambda (hierarchy_level2, n1) : hierarchy_level2)
    
    # optimization for m1_beta_i
    ## OPTIMIZATION : m1_h_draw is already persisted with keyby so we dont need to keyby it : m1_h_draw_previous_iteration = m1_h_draw.keyBy(lambda (iteri, h2, h_draw): h2)
    ##>> OPTMIZATION it is understood that m1_Vbeta_inv_Sigmabeta_j_draw will be previous iteration when assigned and deassigned values for each iteration.
    ##>> ONLY SAVING it for KeyBy TODO COnvert m1_Vbeta_inv_Sigmabeta_j_draw_previous_iteration to collections only and then keep using that one.
    #m1_beta_i_draw.map(gtr.get_beta_i_draw_long).keyBy(lambda (x, h2, h1, beta_i_draw, driver_x_array, hierarchy_level2_hierarchy_level1_driver): x).saveAsNewAPIHadoopFile(hdfs_dir+ "m1_beta_i_draw_long_"+str(1)+".data", "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat","org.apache.hadoop.io.IntWritable")
    # OPTIMIZATION by changing the retunr values of keyby as (s, rows)    
    #m1_beta_i_draw.map(gtr.get_beta_i_draw_long).keyBy(lambda (x, lst): x).saveAsNewAPIHadoopFile(hdfs_dir+ "m1_beta_i_draw_long_"+str(1)+".data", "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat","org.apache.hadoop.io.IntWritable")
    try:    
        m1_beta_i_draw.map(gtr.get_beta_i_draw_long).saveAsPickleFile(hdfs_dir+ "m1_beta_i_draw_long_"+str(1)+".data")
    except:
        print "OOps missed that"
    #keyBy(lambda (x, h2, h1, beta_i_draw, driver_x_array, hierarchy_level2_hierarchy_level1_driver): x).saveAsNewAPIHadoopFile(hdfs_dir+ "m1_beta_i_draw_long_"+str(1)+".data", "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat","org.apache.hadoop.io.IntWritable")
    
    print "Gibbs Iteration Start at ", time.strftime("%a, %d %b %Y %H:%M:%S")
    from datetime import datetime
    start_time = datetime.now()
    
    for s in range(begin_iter, end_iter+1):
        
        ## Inserting into m1_beta_i
        print "Inserting into m1_Vbeta_i"
        
        # Don’t spill to disk unless the functions that computed your datasets are expensive, or they filter a large amount of the data. 
        # Otherwise, recomputing a partition may be as fast as reading it from disk.        
        
        # OPTIMIZATION is to compute next values of m1_Vbeta_i : (h2, [(sequence, hierarchy_level2, hierarchy_level1, Vbeta_i)]) 
        #if s == 2 or s % 11 == 0:
        #    m1_Vbeta_i_p = m1_Vbeta_i
        #else:
        #    ## unify with the previous one
        #    m1_Vbeta_i_p.union(m1_Vbeta_i)
        #m1_Vbeta_inv_Sigmabeta_j_draw_collection = sorted(m1_Vbeta_inv_Sigmabeta_j_draw_collection) 
        ## CHECK IF m1_Vbeta_inv_Sigmabeta_j_draw_collection is sorted & collected in each iteration or not.
        # (hierarchy_level1, hierarchy_level2, xtx, xty) & 
        # m1_Vbeta_inv_Sigmabeta_j_draw_collection  is key by (int(str(hierarchy_level2)[0]), Vbeta_inv_j_draw, sequence, n1,  Sigmabeta_j, h_draw)
        
        ## After Frames optimization -> removing need for collections and persisting on i levels
        try:
            m1_Vbeta_i.unpersist()
        except:
            print "Failed to remove m1_Vbeta_i"
        # pinv_Vbeta_i(xtx, Vbeta_inv_j_draw, h_draw)
        #s, hierarchy_level2, h2_h1_key, Vbeta_i, h_draw, xty, Vbeta_inv_j_draw  
        #m1_Vbeta_i = m1_d_array_agg_constants.keyBy(lambda (h2_h1_key, hierarchy_level2, xt_x, xt_y): hierarchy_level2).join(m1_Vbeta_inv_Sigmabeta_j_draw).map(lambda (hierarchy_level2, y) : (s, hierarchy_level2, y[0][0], gtr.pinv_Vbeta_i(y[0][2], y[1][2], y[1][4]), y[1][4], y[0][3], y[1][2])).persist()
        m1_Vbeta_i = m1_d_array_agg_constants.map(lambda (h2_h1_key, hierarchy_level2, xt_x, xt_y): (s, hierarchy_level2, h2_h1_key, gtr.pinv_Vbeta_i(xt_x, m1_Vbeta_inv_Sigmabeta_j_draw_collection[hierarchy_level2][1], m1_Vbeta_inv_Sigmabeta_j_draw_collection[hierarchy_level2][5]), xt_y), preservesPartitioning = True).persist(storagelevel)    
        #print "count  m1_Vbeta_i_unified   ", m1_Vbeta_i_unified.count()
        #print "take 1 m1_Vbeta_i_unified ", m1_Vbeta_i_unified.take(1)
        ## OPTIMIZATION SAVED m1_Vbeta_i_keyby_h2_h1 = m1_Vbeta_i.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i): (hierarchy_level2, hierarchy_level1))
        #try:
        #    if s % 10 == 0 :            
        #        m1_Vbeta_i_p.saveAsPickleFile(hdfs_dir+ "m1_Vbeta_i_"+str(s)+".data")  
        #        m1_Vbeta_i_p.unpersist()
        #except:
        #    print "OOps missed that"
        #finally:
        #    try:
        #        m1_Vbeta_i_p.unpersist()
        #    except:
        #        print "Exception while unpersisting m1_Vbeta_i_p"
                
        
        ### Inserting into beta_i_mean
        print "Inserting into beta_i_mean"
        
        ##OPTIMIZATION over JOINED #>> above
        ##  (h2, Vbeta_inv_j_draw, sequence, no_n1 Sigmabeta_j, h_draw) is already collected in previous coefficient(m1_Vbeta_i) computation 
        ## we only need beta_mu_j_draw so collecting it over.
        
        m1_beta_mu_j_draw_collection = m1_beta_mu_j_draw.map(lambda (sequence, hierarchy_level2, beta_mu_j_draw, Vbeta_inv_j_draw): (hierarchy_level2, sequence, hierarchy_level2, beta_mu_j_draw, Vbeta_inv_j_draw)).collect()
        #m1_beta_mu_j_draw_collection = sorted(map(lambda (sequence, h2, beta_mu_j_draw, Vbeta_inv_j_draw): (int(str(hierarchy_level2)[0]), sequence, h2, beta_mu_j_draw.all(), Vbeta_inv_j_draw.all()), m1_beta_mu_j_draw_collection))
        # takes 7 + secs to finish.
        m1_beta_mu_j_draw_collection = sorted(m1_beta_mu_j_draw_collection)
        #print "m1_beta_mu_j_draw_collection ", m1_beta_mu_j_draw_collection
        
        ## 
        #if s == 2 or s % 11 == 0:
        #    m1_beta_i_mean_p = m1_beta_i_mean
        #else:
        #    ## unify with the previous one
        #    m1_beta_i_mean_p.union(m1_beta_i_mean)
        ##using the gu function gu.beta_i_mean(Vbeta_i, h_draw, xty, Vbeta_inv_j_draw, beta_mu_j_draw) directly over the following functions as 
        ###>>>m1_beta_i_mean_keyBy_h2_long_next = JOINED_part_1_by_keyBy_h2.cogroup(JOINED_part_2_by_keyBy_h2).map(lambda (x,y): (x, gtr.get_beta_i_mean_next(y, s)))
        # m1_Vbeta_inv_Sigmabeta_j_draw_collection  is key by (int(str(hierarchy_level2)[0]), Vbeta_inv_j_draw, sequence, n1,  Sigmabeta_j, h_draw)
        try:        
            m1_beta_i_mean.unpersist()
        except:
            print "Unpersist m1_beta_i_mean"
        # m1_beta_i_mean is a RDD of (sequence, h2, h1, beta_i_mean, Vbeta_i)
        m1_beta_i_mean = m1_Vbeta_i.map(lambda (sequence, h2, h1, Vbeta_i, xty): (s, h2, h1, gu.beta_i_mean(Vbeta_i, m1_Vbeta_inv_Sigmabeta_j_draw_collection[h2][5], xty,  m1_beta_mu_j_draw_collection[h2][4], m1_beta_mu_j_draw_collection[h2][3]), Vbeta_i), preservesPartitioning = True).persist(storagelevel)
        
                    
        #try:
        #    if s % 10 == 0 :            
        #        m1_beta_i_mean_p.saveAsPickleFile(hdfs_dir+ "m1_beta_i_mean_"+str(s)+".data")
        #        m1_beta_i_mean_p.unpersist()
        #except:
        #    
        #    print "OOps missed that"
        #finally:
        #    try:
        #        m1_beta_i_mean_p.unpersist()
        #    except:
        #        print "Exception while unpersisting m1_beta_i_mean_p"
        # the Unified table is the actual table that reflects all rows of m1_beta_i_draw in correct format.
        # strucutured as iter or s, h2, h1, beta_i_mean
        ## OPTIMIZING THE UNIONIZING of functions and removing it from iterations
        ## m1_beta_i_mean = m1_beta_i_mean.union(sc.parallelize(m1_beta_i_mean_keyBy_h2_long_next.values().reduce(add)))
        #print "count  m1_Vbeta_i_unified   ", m1_beta_i_draw_unified.count()
        #print "take 1 m1_Vbeta_i_unified ", m1_beta_i_draw_unified.take(1)
        # OPTIMIZATIOn : NO NEED FOR USING IT. m1_beta_i_mean_keyBy_h2_h1 = m1_beta_i_mean.keyBy(lambda (sequence, hierarchy_level2, hierarchy_level1, beta_i_mean, Vbeta_i): (hierarchy_level2, hierarchy_level1))
        
        print "insert into beta_i_draw"
        # After cogroup of above two Data Structures we can easily compute bet_draw directly from the map function
        # structure : s, h2, h1, beta_draw 
        # m1_beta_i_draw_long_next is required for computing the Gewke estimations
        
        if s == 2 or s % 11 == 0:
            m1_beta_i_draw_p = m1_beta_i_draw
        else:
            ## unify with the previous one
            m1_beta_i_draw_p.union(m1_beta_i_draw).persist(storagelevel)
        
        ## USING THE OPTIMIZATION OF PREVIOUS init functions where only m1_beta_i_mean_by_current_iteration was used. 
        ##>>m1_beta_i_draw = m1_beta_i_mean_by_current_iteration.cogroup(m1_Vbeta_i_keyby_h2_h1_current_iteration).map(lambda (x,y): (s, x[0], x[1], gu.beta_draw(list(y[0])[0][3], list(y[1])[0][3])), preservesPartitioning = True).persist(storagelevel)
        try:        
            m1_beta_i_draw.unpersist() 
        except:
            print "Unpersist m1_beta_i_draw"
        m1_beta_i_draw = m1_beta_i_mean.map(lambda (sequence, h2, h1, beta_i_mean, Vbeta_i): (s, h2, h1, gu.beta_draw(beta_i_mean, Vbeta_i)), preservesPartitioning = True).persist(storagelevel)
                
        ## Creating vertical draws
        # OPTIMIZATION After Key FOR m1_beta_i_draw_long_next
        ## lists of tuples : s, h2, h1, beta_i_draw[:,i][0], driver_x_array[i], hierarchy_level2_hierarchy_level1_driver
        try:
            if s % 10 == 0 : 
                import pickle
                l = m1_beta_i_draw_p.map(gtr.get_beta_i_draw_long).reduceByKey(add).collect()
                #print "M1_d_longs >>>", l
                #d = dict([(k, v) for k,v in zip (l[0][0][0], l)])
                d = dict(l)
                #d = dict([(k, v) for k,v in zip (l[0][::2], l[0][1::2])])
                #print "M!_d_long d >>>", d
                output = open("/home/ssoni/mmm_t/Code/result/m1_beta_i_draw_"+str(s)+".data",'ab+')
                pickle.dump(d, output) 
                output.close()
                #m1_beta_i_draw_p.map(gtr.get_beta_i_draw_long).keyBy(lambda (x, lst): x).saveAsTextFile(hdfs_dir+ "m1_beta_i_draw_long_tx_"+str(s)+".data")
                # following didnt work  files with no contents               
                #with open("/home/ssoni/mmm_t/Code/result/m1_beta_i_draw_"+str(s)+".data", "a") as f:
                #    f.write(m1)
                #m1_beta_i_draw_p.map(gtr.get_beta_i_draw_long).keyBy(lambda (x, lst): x).saveAsPickleFile(hdfs_dir+ "m1_beta_i_draw_long_"+str(s)+".data")
                #m1_beta_i_draw_p.unpersist()    
        except:
            
            print "OOps Could not write to driver "    
        finally:
            try:
                m1_beta_i_draw_p.unpersist()  
            except:
                print "Unpersist m1_beta_i_draw_p"
        #print "m1_beta_i_draw take ", m1_beta_i_draw.take(1) 
        #print "m1_beta_i_draw count ", m1_beta_i_draw.count()        
        
        # insert into Vbeta_j_mu table 
        print "Inserting into Vbeta_j_mu"
        # using using the most "recent" values of the beta_i_draw coefficients
        # The values of beta_mu_j_draw and Vbeta_j_mu have not yet been updated at this stage, so their values at iter=s-1 are taken.
        # S1 : h2, h1, beta_i_draw  from m1_beta_i_draw where iteri == s ==> m1_beta_i_draw_next => key it by h2
        m1_beta_i_draw_key_by_h2 = m1_beta_i_draw.keyBy(lambda (s, h2, h1, beta_i_draw): h2)
        # S2 : h2, beta_mu_j_draw from m1_beta_mu_j_draw where iter= s-1 and also key it by h2
        # m1_beta_mu_j_draw_by_previous_iteration = hierarchy_level2 -> (s-1, hierarchy_level2, beta_mu_j_draw)
        #m1_beta_mu_j_draw has tuples : iteri, key, gu.beta_draw(beta_mu_j, Sigmabeta_j), Vbeta_inv_j_draw)
        m1_beta_mu_j_draw = m1_beta_mu_j_draw.keyBy(lambda (s_previous, hierarchy_level2, beta_mu_j_draw, Vbeta_inv_j_draw): hierarchy_level2)
        #JOINED_m1_beta_i_draw_next_key_by_h2_WITH_m1_beta_mu_j_draw_by_previous_iteration = m1_beta_i_draw_key_by_h2.cogroup(m1_beta_mu_j_draw).map(lambda (x,y): (x, list(y[0]), list(y[1])[0][2])).groupBy(lambda x : gp.partitionByh2(x), h2_partitions)
        JOINED_m1_beta_i_draw_next_key_by_h2_WITH_m1_beta_mu_j_draw_by_previous_iteration = m1_beta_i_draw_key_by_h2.cogroup(m1_beta_mu_j_draw).map(lambda (x,y): (x, list(y[0]), list(y[1])[0][2])).keyBy(lambda (h2, l1, l2) : h2).groupByKey()
        ## OPTIMIZATION onf JOINED to get it grouped by the GroupedBy clause to build upon further iterations on top of it, which have the same partitioning
        ## .map(lambda (x,y): (x, list(y[0]), list(y[1])[0][1])).groupBy(lambda x : gp.partitionByh2(x[0]), h2_partitions).persist(storagelevel)
        #if s == 2 or s % 11 == 0:
        #    m1_Vbeta_j_mu_p = m1_Vbeta_j_mu
        #else:
        #    ## unify with the previous one
        #    m1_Vbeta_j_mu_p.union(m1_Vbeta_j_mu)
        try:    
            m1_Vbeta_j_mu.unpersist()
        except:
            print "Unpersist m1_Vbeta_j_mu"
        ## NOP : changing s and h2_int's position and making it look like suitable for parttionByCaluse => then persisting in mem? only if required => only if reducing shuffle.
        #m1_Vbeta_j_mu = JOINED_m1_beta_i_draw_next_key_by_h2_WITH_m1_beta_mu_j_draw_by_previous_iteration.map(lambda (h2_int, y): (s, gtr.get_Vbeta_j_mu_next(y, s)), preservesPartitioning = True).persist(storagelevel)
        m1_Vbeta_j_mu = JOINED_m1_beta_i_draw_next_key_by_h2_WITH_m1_beta_mu_j_draw_by_previous_iteration.map(lambda (h2_int, y): (h2_int, (s, gtr.get_Vbeta_j_mu_next(y, s)))).partitionBy(5).persist(storagelevel)
                
        ## OPTIMIZATION no need for unions m1_Vbeta_j_mu = m1_Vbeta_j_mu.union(m1_Vbeta_j_mu_next)
        #print "count  m1_Vbeta_j_mu   ", m1_Vbeta_j_mu.count()
        #print "take 1 m1_Vbeta_j_mu, s ", m1_Vbeta_j_mu.collect(), "WITH S ", s
        #try:            
        #    if s % 10 == 0 :            
        #        m1_Vbeta_j_mu_p.saveAsPickleFile(hdfs_dir+ "m1_Vbeta_j_mu_"+str(s)+".data")
        #        m1_Vbeta_j_mu_p.unpersist()
        #except:
        #    
        #    print "OOps missed that"
        #finally:
        #    try:
        #        m1_Vbeta_j_mu_p.unpersist()  
        #    except:
        #        print "Exception while unpersisting m1_Vbeta_j_mu_p"
        
        ## inserting into m1_Vbeta_inv_Sigmabeta_j_draw
        print "inserting into m1_Vbeta_inv_Sigmabeta_j_draw"
        ## computing Vbeta_inv_j_draw from m1_Vbeta_j_mu where iteration == s
        ## returns s, h2, Vbeta_inv_j_draw
        
        m1_Vbeta_j_mu_pinv = m1_Vbeta_j_mu.map(gtr.get_m1_Vbeta_j_mu_pinv, preservesPartitioning = True)
        #print "count  m1_Vbeta_j_mu_pinv   ", m1_Vbeta_j_mu_pinv.count()
        #print "take 1 m1_Vbeta_j_mu_pinv ", m1_Vbeta_j_mu_pinv.take(1)
        
        # structure m1_d_childcount_groupBy_h2 can be used,
        # m1_d_childcount_groupBy_h2 has a structure h2 -> h2, n1
        #  OPTIMIATION : not cogrouping Vbeta_j_mu tieh childcount as it is another shuffle, instead using the arary 
        # m1_d_childcount which is after sorted and new mod function is a list with elements like :  [(1, 30), (2, 30), (3, 30), (4, 30), (5, 30)]    
        ##>>>JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2 = m1_Vbeta_j_mu_pinv.cogroup(m1_d_childcount_groupBy_h2)
        # s, h2, Vbeta_inv_j_draw, n1
        ##>>>JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified = JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2.map(lambda (x,y): (list(y[0])[0][0], list(y[0])[0][1], list(y[0])[0][2], list(y[1])[0][1]))
        #print "count  JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified   ", JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified.count()
        #print "take 1 JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified ", JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified.take(1)    
        ## optimization directly using the space of Vbeta_j_mu_pinv
        # iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j
        ##>>>m1_Vbeta_inv_Sigmabeta_j_draw_next = JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified.map(gtr.get_m1_Vbeta_inv_Sigmabeta_j_draw_next)
        #if s == 2 or s % 11 == 0:
        #    m1_Vbeta_inv_Sigmabeta_j_draw_p = m1_Vbeta_inv_Sigmabeta_j_draw
        #else:
        #    ## unify with the previous one
        #    m1_Vbeta_inv_Sigmabeta_j_draw_p.union(m1_Vbeta_inv_Sigmabeta_j_draw)
        try:    
            m1_Vbeta_inv_Sigmabeta_j_draw.unpersist()
        except:
            print "Unpersist m1_Vbeta_inv_Sigmabeta_j_draw"
        
        m1_Vbeta_inv_Sigmabeta_j_draw = m1_Vbeta_j_mu_pinv.map(lambda (seq, hierarchy_level2, Vbeta_inv_j_draw): (s, hierarchy_level2, m1_d_childcount.value[hierarchy_level2][1], Vbeta_inv_j_draw, gtr.pinv_Vbeta_inv_Sigmabeta_j_draw(Vbeta_inv_j_draw, m1_d_childcount.value[hierarchy_level2][1], coef_precision_prior_array)), preservesPartitioning = True).persist(storagelevel)
        #try:            
        #    if s % 10 == 0 :            
        #        m1_Vbeta_inv_Sigmabeta_j_draw_p.saveAsPickleFile(hdfs_dir+ "m1_Vbeta_inv_Sigmabeta_j_draw_"+str(s)+".data")
        #        m1_Vbeta_inv_Sigmabeta_j_draw_p.unpersist()
        #except:                
        #    
        #    print "OOps missed that"
        #finally:
        #    try:
        #        m1_Vbeta_inv_Sigmabeta_j_draw_p.unpersist()  
        #    except:
        #        print "Exception while unpersisting m1_Vbeta_inv_Sigmabeta_j_draw_p"       
        
        #print "count  m1_Vbeta_inv_Sigmabeta_j_draw_next   ", m1_Vbeta_inv_Sigmabeta_j_draw.count()
        #print "take 1 m1_Vbeta_inv_Sigmabeta_j_draw_next ", m1_Vbeta_inv_Sigmabeta_j_draw.take(1)
        ## appending the next iteration values to previous Data Structure
        ## NO need for optimizing here.
        ## m1_Vbeta_inv_Sigmabeta_j_draw = m1_Vbeta_inv_Sigmabeta_j_draw.union(m1_Vbeta_inv_Sigmabeta_j_draw_next)
        
        ## inserting into m1_beta_mu_j
        print "Inserting into m1_beta_mu_j"
        # -- Compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  
        # -- Get back one coefficient vector for each j (i.e. J  coefficient vectors are returned).
        # first we modify m1_beta_i_draw to compute sum_coef_j
        # OPTIMIZATION as m1_ols_beta_i_sum_coef_j = m1_ols_beta_i.groupByKey().map(lambda (key, value) : gtr.add_coeff_j(key, value))
        # m1_beta_i_draw_keyby_h2 has a key-value structure h2 -> (s, h2, h1, beta_i_draw) into h2 -> h2, sum_coef_j
        ###>>>m1_beta_i_draw_next_key_by_h2_sum_coef_j = m1_beta_i_draw_next_key_by_h2.map(lambda (x,y): (x, y[3])).keyBy(lambda (h2, coeff): h2).groupByKey().map(lambda (key, value) : gtr.add_coeff_j(key,value))
        m1_beta_i_draw_next_key_by_h2_sum_coef_j = m1_beta_i_draw_key_by_h2.groupByKey().map(lambda (key, value) : gtr.add_coeff_j_next(key,value))
        # NEXT 
        # Join m1_Vbeta_inv_Sigmabeta_j_draw (with iteration == s, i.e., m1_Vbeta_inv_Sigmabeta_j_draw_next, select *) 
        # & m1_beta_i_draw (with iteration == s, i.e., m1_beta_i_draw_next_key_by_h2_sum_coef_j, select h2, sum_coef_j)
        m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2 = m1_Vbeta_inv_Sigmabeta_j_draw.keyBy(lambda (s, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2)
        ## very costly Cogroup
        joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j = m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2.cogroup(m1_beta_i_draw_next_key_by_h2_sum_coef_j)
        
        #if s == 2 or s % 11 == 0:
        #    m1_beta_mu_j_p = m1_beta_mu_j
        #else:
        #    ## unify with the previous one
        #    m1_beta_mu_j_p.union(m1_beta_mu_j)
        # Using same function as was used in the gibbs_init
        # s, h2, beta_mu_j
        try:
            m1_beta_mu_j.unpersist()
        except:
            print "Unpersist m1_beta_mu_j"
        
        #OPTIMIZATION from init iteri, hierarchy_level2, beta_mu_j, Vbeta_inv_j_draw, Sigmabeta_j
        ##NPO : Applying the same partition sense as was applied for Vbeta_j_mu as it is build for same sort of computations.
        ## NPO: now beta_mu_j becomes a key value pair where key is h2 : value is (iteri, hierarchy_level2, beta_mu_j, Vbeta_inv_j_draw, Sigmabeta_j)
        m1_beta_mu_j = joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j.map(lambda (x, y): (x, gtr.get_substructure_beta_mu_j(y))).partitionBy(5).persist(storagelevel)
        #m1_beta_mu_j = joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j.map(gtr.get_substructure_beta_mu_j, preservesPartitioning = True).persist(storagelevel)
        ## OPTIMIZATION : NO NEED for unions m1_beta_mu_j = m1_beta_mu_j.union(m1_beta_mu_j_next)
        #print "count  m1_beta_mu_j_next   ", m1_beta_mu_j.count()
        #print "take 1 m1_beta_mu_j_next ", m1_beta_mu_j.take(1)
        # h2 -> s, h2, beta_mu_j
        # Beta_mu_j keyed by h2
        # m1_beta_mu_j_keyBy_h2 = m1_beta_mu_j.keyBy(lambda (iter, hierarchy_level2, beta_mu_j): hierarchy_level2)
        #try:
        #    if s % 10 == 0 :            
        #        m1_beta_mu_j_p.saveAsPickleFile(hdfs_dir+ "m1_beta_mu_j_"+str(s)+".data")
        #        m1_beta_mu_j_p.unpersist()
        #except:
        #    
        #    print "OOps missed that"
        #finally:
        #    try:
        #        m1_beta_mu_j_p.unpersist()  
        #    except:
        #        print "Exception while unpersisting m1_beta_mu_j_p" 
        # inserting into m1_beta_mu_j_draw
        # -- Draw beta_mu from mvnorm dist'n.  Get back J vectors of beta_mu, one for each J.  Note that all input values are at iter=s.
        print "inserting into m1_beta_mu_j_draw"
        # Structures : m1_beta_mu_j and m1_Vbeta_inv_Sigmabeta_j_draw for current iteration will be cogrouped.
        # m1_beta_mu_j_next : s, h2, beta_mu_j, we will need h2, beta_mu_j
        # m1_Vbeta_inv_Sigmabeta_j_draw_next : s, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j, we will need h2, Sigmabeta_j
        # OPTIMIZATION saved over cogroup.
        #m1_beta_mu_j_next_keyBy_h2 = m1_beta_mu_j.keyBy(lambda (s, h2, beta_mu_j): h2)
        # now the cogroup
        #Joined_m1_beta_mu_j_next_keyBy_h2_WITH_m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2 = m1_beta_mu_j_next_keyBy_h2.cogroup(m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2)
        #if s == 2 or s % 11 == 0:
        #    m1_beta_mu_j_draw_p = m1_beta_mu_j_draw
        #else:
        #    ## unify with the previous one
        #    m1_beta_mu_j_draw_p.union(m1_beta_mu_j_draw)
        try:    
            m1_beta_mu_j_draw.unpersist()
        except:
            print "Unpersist m1_beta_mu_j_draw"
        #m1_beta_mu_j_draw = Joined_m1_beta_mu_j_next_keyBy_h2_WITH_m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2.map(gtr.get_beta_draw, preservesPartitioning = True).persist(storagelevel)
        ## NOP h2 -> (values) where values or y : seq, hierarchy_level2, beta_mu_j, Vbeta_inv_j_draw, Sigmabeta_j
        m1_beta_mu_j_draw = m1_beta_mu_j.map(lambda (hierarchy_level2, y): (s, hierarchy_level2, gu.beta_draw(y[2], y[4]), y[3]), preservesPartitioning = True).persist(storagelevel)
        
        #m1_beta_mu_j_draw = m1_beta_mu_j.map(lambda (seq, hierarchy_level2, beta_mu_j, Vbeta_inv_j_draw, Sigmabeta_j): (seq, hierarchy_level2, gu.beta_draw(beta_mu_j, Sigmabeta_j), Vbeta_inv_j_draw), preservesPartitioning = True).persist(storagelevel)
        #print "count  m1_beta_mu_j_draw_next   ", m1_beta_mu_j_draw_next.count()
        #print "take 1 m1_beta_mu_j_draw_next ", m1_beta_mu_j_draw_next.take(1)
        #OPTIMIZATION : NO NEED for unions m1_beta_mu_j_draw = m1_beta_mu_j_draw.union(m1_beta_mu_j_draw_next)
        # beta_mu_j_draw keyed by h2
        #m1_beta_mu_j_draw = m1_beta_mu_j_draw_next.keyBy(lambda (iter, hierarchy_level2, beta_mu_j_draw, Vbeta_inv_j_draw): hierarchy_level2)
        #try:
        #    if s % 10 == 0 :            
        #        m1_beta_mu_j_draw_p.saveAsPickleFile(hdfs_dir+ "m1_beta_mu_j_draw_"+str(s)+".data")
        #        m1_beta_mu_j_draw_p.unpersist()
        #except:
        #    
        #    print "OOps missed that"
        #finally:
        #    try:
        #        m1_beta_mu_j_draw_p.unpersist()  
        #    except:
        #        print "Exception while unpersisting m1_beta_mu_j_draw_p"    
            
        # Update values of s2
        ##-- Compute updated value of s2 to use in next section.
        print "Updating values of s2"
        m1_beta_i_draw_group_by_h2_h1 = m1_beta_i_draw.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, beta_i_draw): (hierarchy_level2, hierarchy_level1))
        # The structure m1_beta_i_draw_next doesnt change during iterations. 
        # m1_beta_i_draw_next is already computed in Gibbs_init via : m1_d_array_agg_key_by_h2_h1 = m1_d_array_agg.keyBy(lambda (h2_h1_key, hierarchy_level2, x_matrix, y_array) : (hierarchy_level2, h2_h1_key))
        JOINED_m1_beta_i_draw_WITH_m1_d_array_agg = m1_d_array_agg_key_by_h2_h1.cogroup(m1_beta_i_draw_group_by_h2_h1)
        #print "JOINED_m1_beta_i_draw_WITH_m1_d_array_agg : 2 : ", JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.take(1)
        # hierarchy_level2, hierarchy_level1, x_array_var, y_var, iter, beta_i_draw
        #JOINED_m1_beta_i_draw_WITH_m1_d_array_agg = JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.map(lambda (x, y): (h2,  h2,   x_matrix,         y_array ,         s,       beta_i_draw,      m1_d_count_grpby_level2_b.value[x[0]]))
        JOINED_m1_beta_i_draw_WITH_m1_d_array_agg = JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.map(lambda (x, y): (x[0], x[1], list(y[0])[0][2], list(y[0])[0][3] , s , list(y[1])[0][3], m1_d_count_grpby_level2_b.value[x[0]]))
            
        #print "JOINED_m1_beta_i_draw_WITH_m1_d_array_agg : 3 :", JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.take(1)
        #JOINED_m1_beta_i_draw_WITH_m1_d_array_agg = JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.map(lambda (x, y): (x[0], x[1], list(list(y[0])[0])[1], list(list(y[0])[0])[2], list(y[1])[0][0], list(y[1])[0][3], m1_d_count_grpby_level2_b.value[x[0]]))
        foo = JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.keyBy(lambda (hierarchy_level2, hierarchy_level1, x_array_var, y_var, iteri, beta_i_draw, m1_d_count_grpby_level2_b): (hierarchy_level2, hierarchy_level1, iteri))
        #print "foo : 4 : ", foo.take(1)
        #print "foo : 4 : ", foo.count()
        # foo2 is group by hierarchy_level2, hierarchy_level1, iteri and has structure as ey => ( hierarchy_level2, hierarchy_level1, iteri, ssr, m1_d_count_grpby_level2_b )
        foo2 = foo.map(lambda (x, y): gtr.get_sum_beta_i_draw_x2(y)).keyBy(lambda (hierarchy_level2, hierarchy_level1, iteri, ssr, m1_d_count_grpby_level2_b): (hierarchy_level2, iteri, m1_d_count_grpby_level2_b))
        #print "foo2 : 5 : ", foo2.take(1)
        #print "foo2 : 5 : ", foo2.count()
        
        #foo3 = foo2.groupByKey().map(lambda (x, y): get_s2(list(y)))
        # iteri, hierarchy_level2, m1_d_count_grpby_level2_b, s2
        m1_s2 = foo2.groupByKey().map(lambda (x, y): gtr.get_s2(list(y)))
        # OPTIMIZATION no need for union witht he previous step as it is not required in further iterations 
        # it is computed new each time.
        ##?}>>>m1_s2 = m1_s2.union(m1_s2_next)
        #print "m1_s2 : ", m1_s2.take(1)
        #print "m1_s2 : ", m1_s2.count()
        
        ## Updating values of h_draw based on current iteration
        # -- Draw h from gamma dist'n.  Note that h=1/(s^2)
        ## from iteri, hierarchy_level2, m1_d_count_grpby_level2_b, s2
        ## m1_h_draw = iteri, h2, h_draw
        #if s == 2 or s % 11 == 0:
        #    m1_h_draw_p = m1_h_draw
        #else:
        #    ## unify with the previous one
        #    m1_h_draw_p.union(m1_h_draw)
            
        #OPT 10000 -> very small var in size so leaving it as is
        #m1_h_draw.unpersist()
        m1_h_draw = m1_s2.map(gtr.get_h_draw).keyBy(lambda (iteri, h2, h_draw): h2).persist(storagelevel)
        # optimization we dont need to persist the previous draws with new ones 
        ## so removing the persistence and creating new persistence
        # OPTIMIZATION : SAVED over unions , only next values used in iterations m1_h_draw = m1_h_draw.union(m1_h_draw_next)
        # iteri, h2, h_draw
        #print "m1_h_draw : ", m1_h_draw.take(1)
        #try:
        #    if s % 10 == 0 :            
        #        m1_h_draw_p.saveAsPickleFile(hdfs_dir+ "m1_h_draw_"+str(s)+".data")
        #        m1_h_draw_p.unpersist()
        #except:
        #    
        #   print "OOps missed that"
        #finally:
        #    try:
        #        m1_h_draw_p.unpersist()  
        #    except:
        #        print "Exception while unpersisting m1_h_draw_p"    
        # Reassigning the collections values
        
            
        m1_Vbeta_inv_Sigmabeta_j_draw_h_draws = m1_Vbeta_inv_Sigmabeta_j_draw.keyBy(lambda (sequence, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2).join(m1_h_draw).map(lambda (h2, y): (y[0][0], h2, y[0][2], y[0][3], y[0][4], y[1][2]))
        
        m1_Vbeta_inv_Sigmabeta_j_draw_collection = m1_Vbeta_inv_Sigmabeta_j_draw_h_draws.map(lambda (sequence, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j, h_draw): (hierarchy_level2, Vbeta_inv_j_draw, sequence, n1,  Sigmabeta_j, h_draw)).collect()
        # (1,<objc>)(2,<objc>)...
        #m1_Vbeta_inv_Sigmabeta_j_draw_collection = sorted(map(lambda (sequence, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): (int(str(hierarchy_level2)[0]), Vbeta_inv_j_draw.all(), sequence, n1,  Sigmabeta_j.all()), m1_Vbeta_inv_Sigmabeta_j_draw_collection))
        m1_Vbeta_inv_Sigmabeta_j_draw_collection = sorted(m1_Vbeta_inv_Sigmabeta_j_draw_collection)
        try:        
            m1_s2.unpersist()
        except:
            print "Unpersist m1_s2"
        
        #print "m1_h_draw : ", s, " ", m1_h_draw.count()

        ## -- Convert the array-based draws from the Gibbs Sampler into a "vertically long" format by unnesting the arrays.
              
        #m1_beta_i_draw_long_next = m1_beta_i_draw.map(gtr.get_beta_i_draw_long).reduce(add)
        #print "reduce count ", len(m1_beta_i_draw_long_next)
        #print "reduce ", m1_beta_i_draw_long_next[0]
        #m1_beta_i_draw_long = m1_beta_i_draw_long + m1_beta_i_draw_long_next
        print "end iteration", s    
    
    end_time = datetime.now()
    print "End of Iteration statistics"
    print 'Duration: ', (end_time - start_time)    
    #    stop = timeit.default_timer()
    #    print "Finished Gibbs Iteration in ", str(start - stop)    
    print gibbs_iteration_text()
    
    return (m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount, m1_d_count_grpby_level2 ,m1_h_draw  ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_j_mu)
