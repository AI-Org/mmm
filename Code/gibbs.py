# -*- coding: utf-8 -*-
"""
@author: ssoni
"""

import gibbs_udfs as gu
import gibbs_transformations as gtr
from pyspark.storagelevel import StorageLevel
import gibbs_partitions as gp

def gibbs_iteration_text():
    text_output = 'Done: All requested Gibbs Sampler updates are complete.  All objects associated with this model are named with a m1 prefix.'   
    return text_output

def add(x,y):
    return (x+y)

#                  d, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, begin_iter, end_iter
def gibbs_iter(sc, begin_iter, end_iter, coef_precision_prior_array, h2_partitions, m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_inv_Sigmabeta_j_draw_collection, m1_Vbeta_j_mu):
    
    m1_beta_mu_j_draw = m1_beta_mu_j_draw.keyBy(lambda (iter, hierarchy_level2, beta_mu_j_draw): hierarchy_level2)
    
    m1_d_array_agg_key_by_h2_h1 = m1_d_array_agg.keyBy(lambda (keys_from_partitioning, x_matrix, y_array, hierarchy_level2, hierarchy_level1) : (hierarchy_level2, hierarchy_level1))
    # OPTIMIZED into grpby_level2    
    m1_d_count_grpby_level2_b = m1_d_count_grpby_level2
    # m1_d_array_agg_constants_key_by_h2_h1 is a large Data Structure which can be persisted across multiple iterations of Gibbs Algorithm
    # Data tuples which will be joined/cogrouped with this data set will be pickled and transferred to each of these nodes carrying 135 partitions.
    ##m1_d_array_agg_constants_key_by_h2_h1 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2, h1)).partitionBy(150).persist()
    ##m1_d_array_agg_constants_key_by_h2 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2)).partitionBy(5).persist()
    # We dont need to partition m1_d_childcount_groupBy_h2 as it is a small data structure which can be shipped to each node already having the persisted data.
    ##m1_d_childcount_groupBy_h2 = m1_d_childcount.keyBy(lambda (hierarchy_level2, n1) : hierarchy_level2)
    
    # optimization for m1_beta_i
    ## OPTIMIZATION : m1_h_draw is already persisted with keyby so we dont need to keyby it : m1_h_draw_previous_iteration = m1_h_draw.keyBy(lambda (iteri, h2, h_draw): h2)
    ##>> OPTMIZATION it is understood that m1_Vbeta_inv_Sigmabeta_j_draw will be previous iteration when assigned and deassigned values for each iteration.
    ##>> ONLY SAVING it for KeyBy TODO COnvert m1_Vbeta_inv_Sigmabeta_j_draw_previous_iteration to collections only and then keep using that one.
    
    
    for s in range(begin_iter, end_iter+1):
        
        ## Inserting into m1_beta_i
        print "Inserting into m1_beta_i"
        ## optimization for m1_h_draw_filter_by_iteration
        ## m1_h_draw = iteri, h2, h_draw
        # instead of using m1_h_draw_filter_by_iteration we can use the m1_h_draw_previous_iteration which is already keyed by h2.
        # m1_h_draw_previous_iteration = m1_h_draw_filter_by_iteration = m1_h_draw.filter(lambda (iteri, h2, h_draw): iteri == s - 1 ).keyBy(lambda (iteri, h2, h_draw): h2)
        #print "m1_h_draw_filter_by_iteration", m1_h_draw_filter_by_iteration.collect()
        
        # applying the same previous iteration trick here that we applied for m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration  
        # We can use m1_Vbeta_inv_Sigmabeta_j_draw_previous_iteration instead of m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration
        # m1_Vbeta_inv_Sigmabeta_j_draw_previous_iteration = m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration = m1_Vbeta_inv_Sigmabeta_j_draw.filter(lambda (iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): iteri == s - 1 ).keyBy(lambda (iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2)
        
        m1_Vbeta_inv_Sigmabeta_j_draw_key_by_h2 = m1_Vbeta_inv_Sigmabeta_j_draw.keyBy(lambda (iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2)      
        # filter m1_h_draw taking only |s| -1 into account
        m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration_join_m1_h_draw_filter_by_iteration = m1_Vbeta_inv_Sigmabeta_j_draw_key_by_h2.cogroup(m1_h_draw)
        
        # Creating a new structure joined_simplified : iteri, h2, Vbeta_inv_j_draw, h_draw, which is a simplified version of what the original structure looks.
        ## OPTIMIZATION lets collect it and distribute over m1_d_array_constants
        # Following is (1,<objc>)(2,<objc>)... where each objc is (h2, Vbeta_inv_j_draw, sequence, no_n1 Sigmabeta_j, h_draw)
        joined_simplified = m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration_join_m1_h_draw_filter_by_iteration.map(lambda (x,y): (int(str(x)[0]), list(y[0])[0][3], list(y[0])[0][0], list(y[0])[0][4], list(y[1])[0][2]), preservesPartitioning = True).collect()
        m1_Vbeta_inv_Sigmabeta_j_draw_collection = sorted(m1_Vbeta_inv_Sigmabeta_j_draw_collection) 
        #>>joined_simplified_key_by_h2 = joined_simplified.keyBy(lambda (iteri, h2, Vbeta_inv_j_draw, h_draw): h2)
        #??joined_simplified_key_by_h2_collection = joined_simplified_key_by_h2.collect()
        ## m1_d_array_agg_constants is RDD of tuples h2, h1, xtx, xty
        ## joined_simplified is RDD of tuples h2 -> iteri, h2, Vbeta_inv_j_draw, h_draw 
        # join the m1_d_array_agg_constants_key_by_h2 with join_simplified
        #>>>m1_d_array_agg_constants_key_by_h2_join_joined_simplified = m1_d_array_agg_constants_key_by_h2.cogroup(joined_simplified_key_by_h2)
        #print "count and take 1", m1_d_array_agg_constants_key_by_h2_join_joined_simplified.count(), m1_d_array_agg_constants_key_by_h2_join_joined_simplified.take(1)
        # following two lines of mapped RDD are for testing : m1_d_array_agg_constants_key_by_h2_join_joined_simplified
        #m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped = m1_d_array_agg_constants_key_by_h2_join_joined_simplified.map(lambda (x,y): (x, list(y[0])[0], list(y[1])[0]))
        #print "m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped", m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped.take(1)
        
        # Don’t spill to disk unless the functions that computed your datasets are expensive, or they filter a large amount of the data. 
        # Otherwise, recomputing a partition may be as fast as reading it from disk.        
        
        # OPTIMIZATION is to compute next values of m1_Vbeta_i : (h2, [(sequence, hierarchy_level2, hierarchy_level1, Vbeta_i)]) 
        # pinv_Vbeta_i(xtx, Vbeta_inv_j_draw, h_draw):
        # to make m1_Vbeta_i as (sequence, h2, h1, Vbeta_i, xty)
        m1_Vbeta_i.unpersist()
        m1_Vbeta_i = m1_d_array_agg_constants.map(lambda (hierarchy_level2, hierarchy_level1, xtx, xty): (s, hierarchy_level2, hierarchy_level1, gtr.pinv_Vbeta_i(xtx, m1_Vbeta_inv_Sigmabeta_j_draw_collection[int(str(hierarchy_level2)[0]) -1][1], m1_Vbeta_inv_Sigmabeta_j_draw_collection[int(str(hierarchy_level2)[0]) -1][4]), xty), preservesPartitioning = True).persist()    
        #>>>m1_Vbeta_i_keyBy_h2_long_next = m1_d_array_agg_constants_key_by_h2_join_joined_simplified.map(lambda (x,y): (x, gtr.get_Vbeta_i_next(y, s)))
        # the Unified table is the actual table that reflects all the rows of m1_Vbeta_i in correct format.
        ##>>>m1_Vbeta_i_keyBy_h2_long_next_cached = sc.parallelize(m1_Vbeta_i_keyBy_h2_long_next.values().reduce(add),135)
        ###>>>m1_Vbeta_i = m1_Vbeta_i.union(m1_Vbeta_i_keyBy_h2_long_next_cached)
        #print "count  m1_Vbeta_i_unified   ", m1_Vbeta_i_unified.count()
        #print "take 1 m1_Vbeta_i_unified ", m1_Vbeta_i_unified.take(1)
        m1_Vbeta_i_keyby_h2_h1 = m1_Vbeta_i.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i): (hierarchy_level2, hierarchy_level1))
        ## OPTIMIZATION OF Previous functions as
    
       
        ### Inserting into beta_i_mean
        print "Inserting into beta_i_mean"
        #m1_Vbeta_i_unified_filter_iter_s = m1_Vbeta_i.filter(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i) : i == s)
        # for m1_Vbeta_i_keyby_h2_h1_current_iteration dont want to persist the current iterations in memory as it is a relatively small data structure and can be moved across nodes where keyed partitions live
        #m1_Vbeta_i_keyby_h2_h1_current_iteration = m1_Vbeta_i_keyBy_h2_long_next_cached.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i, xty): (hierarchy_level2, hierarchy_level1))
        #print "count  m1_Vbeta_i_keyby_h2_h1   ", m1_Vbeta_i_keyby_h2_h1.count()
        #print "take 1 m1_Vbeta_i_keyby_h2_h1 ", m1_Vbeta_i_keyby_h2_h1.take(1)    
        #  JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1 of tuples : hierarchy_level2, hierarchy_level1, Vbeta_i,xty 
        # following is the foo of computer beta_i_mean
        ##OPTIMIZED INTO m1_Vbeta_i_keyby_h2_h1_current_iteration itself 
        ##>>>JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1 = m1_Vbeta_i_keyby_h2_h1_current_iteration.cogroup(m1_d_array_agg_constants_key_by_h2_h1).map(lambda (x,y): (list(y[0])[0][1], list(y[0])[0][2], list(y[0])[0][3], list(y[1])[0][3]))
        ##>>>JOINED_part_1_by_keyBy_h2 = JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1.keyBy(lambda (hierarchy_level2, hierarchy_level1, Vbeta_i, xty): hierarchy_level2)
        
        # m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration joined with m1_beta_mu_j_draw_by_previous_iteration
        # m1_beta_mu_j_draw = hierarchy_level2 -> (iter, hierarchy_level2, beta_mu_j_draw)
        # OPTIMIZATION m1_beta_mu_j_draw is actually the previous iteration itself
        #m1_beta_mu_j_draw_by_previous_iteration = m1_beta_mu_j_draw_keyBy_h2.filter(lambda (x,y): y[0] == s - 1)
        #print "count  m1_beta_mu_j_draw_by_previous_iteration   ", m1_beta_mu_j_draw_by_previous_iteration.count()
        #print "take 1 m1_beta_mu_j_draw_by_previous_iteration ", m1_beta_mu_j_draw_by_previous_iteration.take(1)
        # Following is computed from h2 ->(resIter1, resIter2) <=> where 
        # resIter1 is (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j)
        # resIter2 is (iter, hierarchy_level2, beta_mu_j_draw)
        # strucuture is  ( h2, iter, Vbeta_inv_j_draw, beta_mu_j_draw )
        ##>>JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw = m1_Vbeta_inv_Sigmabeta_j_draw_previous_iteration.cogroup(m1_beta_mu_j_draw).map(lambda (x,y): (x, list(y[0])[0][0], list(y[0])[0][3], list(y[1])[0][2])).keyBy(lambda (hierarchy_level2, iteri, Vbeta_inv_j_draw, beta_mu_j_draw): hierarchy_level2)
        # Cogroup above structure h2 -> h2, iter, Vbeta_inv_j_draw, beta_mu_j_draw with m1_h_draw_filter_by_iteration,  h2 -> (iteri, h2, h_draw)
        ###>>>JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw_join_WITH_h_draw = JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.cogroup(m1_h_draw).map(lambda (x, y): (list(y[0])[0][0], list(y[1])[0][2], list(y[0])[0][2], list(y[0])[0][3]))
        ###>>>JOINED_part_2_by_keyBy_h2 = JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw_join_WITH_h_draw.keyBy(lambda (hierarchy_level2, h_draw, Vbeta_inv_j_draw, beta_mu_j_draw): hierarchy_level2)
        #print "take 1 ", JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw_join_WITH_h_draw.take(1) 
        #print "count 1 ", JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw_join_WITH_h_draw.count()
        
        ##OPTIMIZATION over JOINED #>> above
        ##  (h2, Vbeta_inv_j_draw, sequence, no_n1 Sigmabeta_j, h_draw) is already collected in previous coefficient(m1_Vbeta_i) computation 
        ## we only need beta_mu_j_draw so collecting it over.
        m1_beta_mu_j_draw_collection = m1_beta_mu_j_draw.map(lambda (sequence, hierarchy_level2, beta_mu_j_draw, Vbeta_inv_j_draw): (int(str(hierarchy_level2)[0]), sequence, hierarchy_level2, beta_mu_j_draw, Vbeta_inv_j_draw), preservesPartitioning = True).collect()
        #m1_beta_mu_j_draw_collection = sorted(map(lambda (sequence, h2, beta_mu_j_draw, Vbeta_inv_j_draw): (int(str(hierarchy_level2)[0]), sequence, h2, beta_mu_j_draw.all(), Vbeta_inv_j_draw.all()), m1_beta_mu_j_draw_collection))
        m1_beta_mu_j_draw_collection = sorted(m1_beta_mu_j_draw_collection)
        print "m1_beta_mu_j_draw_collection ", m1_beta_mu_j_draw_collection
        
        ## using the gu function gu.beta_i_mean(Vbeta_i, h_draw, xty, Vbeta_inv_j_draw, beta_mu_j_draw) directly over the following functions as 
        ###>>>m1_beta_i_mean_keyBy_h2_long_next = JOINED_part_1_by_keyBy_h2.cogroup(JOINED_part_2_by_keyBy_h2).map(lambda (x,y): (x, gtr.get_beta_i_mean_next(y, s)))
        m1_beta_i_mean.unpersist()
        m1_beta_i_mean = m1_Vbeta_i.map(lambda (sequence, h2, h1, Vbeta_i, xty): (sequence, h2, h1, gu.beta_i_mean(Vbeta_i, m1_Vbeta_inv_Sigmabeta_j_draw_collection[int(str(h2)[0]) -1][4], xty,  m1_beta_mu_j_draw_collection[int(str(h2)[0]) -1][4], m1_beta_mu_j_draw_collection[int(str(h2)[0]) -1][3]), m1_beta_mu_j_draw_collection[int(str(h2)[0]) -1][4]), preservesPartitioning = True).persist()
    
        # the Unified table is the actual table that reflects all rows of m1_beta_i_draw in correct format.
        # strucutured as iter or s, h2, h1, beta_i_mean
        ## OPTIMIZING THE UNIONIZING of functions and removing it from iterations
        ## m1_beta_i_mean = m1_beta_i_mean.union(sc.parallelize(m1_beta_i_mean_keyBy_h2_long_next.values().reduce(add)))
        #print "count  m1_Vbeta_i_unified   ", m1_beta_i_draw_unified.count()
        #print "take 1 m1_Vbeta_i_unified ", m1_beta_i_draw_unified.take(1)
        m1_beta_i_mean_keyBy_h2_h1 = m1_beta_i_mean.keyBy(lambda (sequence, hierarchy_level2, hierarchy_level1, beta_i_mean): (hierarchy_level2, hierarchy_level1))
        
        # insert into beta_i, using iter=s-1 values.  Draw from mvnorm dist'n.
        #print "insert into beta_i"
        # for strucuter like iter, h2, h1, beta_draw,
        # ,for iter for iter == s, join beta_i_mean and Vbeta_i for structure s, h2, h1, beta_i_mean, Vbeta_i
        #m1_beta_i_mean_by_current_iteration = m1_beta_i_mean_keyBy_h2_h1.filter(lambda (x,y): y[0] == s)
        
        # In place of this computation >>>m1_Vbeta_i_by_current_iteration = m1_Vbeta_i_keyby_h2_h1.filter(lambda (x,y): y[0] == s)
        # we can use m1_Vbeta_i_keyby_h2_h1_current_iteration         
        
        print "insert into beta_i_draw"
        # After cogroup of above two Data Structures we can easily compute bet_draw directly from the map function
        # structure : s, h2, h1, beta_draw 
        m1_beta_i_draw_long_next = m1_beta_i_draw.map(gtr.get_beta_i_draw_long).reduce(add)
        m1_beta_i_draw.unpersist()
        ## USING THE OPTIMIZATION OF PREVIOUS init functions where only m1_beta_i_mean_by_current_iteration was used. 
        ##>>m1_beta_i_draw = m1_beta_i_mean_by_current_iteration.cogroup(m1_Vbeta_i_keyby_h2_h1_current_iteration).map(lambda (x,y): (s, x[0], x[1], gu.beta_draw(list(y[0])[0][3], list(y[1])[0][3])), preservesPartitioning = True).persist()
        m1_beta_i_draw = m1_beta_i_mean_keyBy_h2_h1.cogroup(m1_Vbeta_i_keyby_h2_h1).map(lambda (x,y): (s, x[0], x[1], gu.beta_draw(list(y[0])[0][3], list(y[1])[0][3])), preservesPartitioning = True).persist()
        #print "count  m1_beta_i_draw_next   ", m1_beta_i_draw_next.count()
        #print "take 1 m1_beta_i_draw_next ", m1_beta_i_draw_next.take(1)
        #Optimization on union save m1_beta_i_draw = m1_beta_i_draw.union(m1_beta_i_draw_next)
        #print "count  m1_beta_i_draw   ", m1_beta_i_draw.count()
        #print "take 1 m1_beta_i_draw ", m1_beta_i_draw.take(1)
        
        # insert into Vbeta_j_mu table 
        print "Inserting into Vbeta_j_mu"
        # using using the most "recent" values of the beta_i_draw coefficients
        # The values of beta_mu_j_draw and Vbeta_j_mu have not yet been updated at this stage, so their values at iter=s-1 are taken.
        # S1 : h2, h1, beta_i_draw  from m1_beta_i_draw where iteri == s ==> m1_beta_i_draw_next => key it by h2
        m1_beta_i_draw_key_by_h2 = m1_beta_i_draw.keyBy(lambda (s, h2, h1, beta_i_draw): h2)
        # S2 : h2, beta_mu_j_draw from m1_beta_mu_j_draw where iter= s-1 and also key it by h2
        # m1_beta_mu_j_draw_by_previous_iteration = hierarchy_level2 -> (s-1, hierarchy_level2, beta_mu_j_draw)
        JOINED_m1_beta_i_draw_next_key_by_h2_WITH_m1_beta_mu_j_draw_by_previous_iteration = m1_beta_i_draw_key_by_h2.cogroup(m1_beta_mu_j_draw).map(lambda (x,y): (x, list(y[0]), list(y[1])[0][2])).groupBy(lambda x : gp.partitionByh2(x[0]), h2_partitions).persist()
        ## OPTIMIZATION onf JOINED to get it grouped by the GroupedBy clause to build upon further iterations on top of it, which have the same partitioning
        ## .map(lambda (x,y): (x, list(y[0]), list(y[1])[0][1])).groupBy(lambda x : gp.partitionByh2(x[0]), h2_partitions).persist()
        m1_Vbeta_j_mu.unpersist()
        m1_Vbeta_j_mu = JOINED_m1_beta_i_draw_next_key_by_h2_WITH_m1_beta_mu_j_draw_by_previous_iteration.map(lambda (h2, y): (s, h2, gtr.get_Vbeta_j_mu_next(y)), preservePartitioning = True).persist()
        #print "count  m1_Vbeta_j_mu_next   ", m1_Vbeta_j_mu_next.count()
        #print "take 1 m1_Vbeta_j_mu_next ", m1_Vbeta_j_mu_next.take(1)
        ## OPTIMIZATION no need for unions m1_Vbeta_j_mu = m1_Vbeta_j_mu.union(m1_Vbeta_j_mu_next)
        #print "count  m1_Vbeta_j_mu   ", m1_Vbeta_j_mu.count()
        #print "take 1 m1_Vbeta_j_mu ", m1_Vbeta_j_mu.take(1)
        
        ## inserting into m1_Vbeta_inv_Sigmabeta_j_draw
        print "inserting into m1_Vbeta_inv_Sigmabeta_j_draw"
        ## computing Vbeta_inv_j_draw from m1_Vbeta_j_mu where iteration == s
        ## returns s, h2, Vbeta_inv_j_draw
        
        m1_Vbeta_j_mu_pinv = m1_Vbeta_j_mu.map(gtr.get_m1_Vbeta_j_mu_pinv, preservePartitioning = True).keyBy(lambda (s, h2, Vbeta_inv_j_draw): h2)
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
        m1_Vbeta_inv_Sigmabeta_j_draw.unpersist()
        m1_Vbeta_inv_Sigmabeta_j_draw = m1_Vbeta_j_mu_pinv.map(lambda (seq, hierarchy_level2, Vbeta_inv_j_draw): (seq, hierarchy_level2, m1_d_childcount[int(str(hierarchy_level2)[0]) -1][1], Vbeta_inv_j_draw, gtr.pinv_Vbeta_inv_Sigmabeta_j_draw(Vbeta_inv_j_draw, m1_d_childcount[int(str(hierarchy_level2)[0]) -1][1], coef_precision_prior_array)), preservesPartitioning = True).persist()
              
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
        m1_beta_i_draw_next_key_by_h2_sum_coef_j = m1_beta_i_draw_key_by_h2.groupByKey().map(lambda (key, value) : gtr.add_coeff_j(key,value))
        # NEXT 
        # Join m1_Vbeta_inv_Sigmabeta_j_draw (with iteration == s, i.e., m1_Vbeta_inv_Sigmabeta_j_draw_next, select *) 
        # & m1_beta_i_draw (with iteration == s, i.e., m1_beta_i_draw_next_key_by_h2_sum_coef_j, select h2, sum_coef_j)   
        joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j = m1_Vbeta_inv_Sigmabeta_j_draw.keyBy(lambda (iter, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2).cogroup(m1_beta_i_draw_next_key_by_h2_sum_coef_j)
        # Using same function as was used in the gibbs_init
        # s, h2, beta_mu_j
        m1_beta_mu_j.unpersist()
        m1_beta_mu_j = joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j.map(gtr.get_substructure_beta_mu_j, preservesPartitioning = True).persist()
        ## OPTIMIZATION : NO NEED for unions m1_beta_mu_j = m1_beta_mu_j.union(m1_beta_mu_j_next)
        #print "count  m1_beta_mu_j_next   ", m1_beta_mu_j.count()
        #print "take 1 m1_beta_mu_j_next ", m1_beta_mu_j.take(1)
        # h2 -> s, h2, beta_mu_j
        # Beta_mu_j keyed by h2
        # m1_beta_mu_j_keyBy_h2 = m1_beta_mu_j.keyBy(lambda (iter, hierarchy_level2, beta_mu_j): hierarchy_level2)
        
        # inserting into m1_beta_mu_j_draw
        # -- Draw beta_mu from mvnorm dist'n.  Get back J vectors of beta_mu, one for each J.  Note that all input values are at iter=s.
        print "inserting into m1_beta_mu_j_draw"
        # Structures : m1_beta_mu_j and m1_Vbeta_inv_Sigmabeta_j_draw for current iteration will be cogrouped.
        # m1_beta_mu_j_next : s, h2, beta_mu_j, we will need h2, beta_mu_j
        # m1_Vbeta_inv_Sigmabeta_j_draw_next : s, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j, we will need h2, Sigmabeta_j
        m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2 = m1_Vbeta_inv_Sigmabeta_j_draw.keyBy(lambda (s, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2)
        m1_beta_mu_j_next_keyBy_h2 = m1_beta_mu_j.keyBy(lambda (s, h2, beta_mu_j): h2)
        # now the cogroup
        Joined_m1_beta_mu_j_next_keyBy_h2_WITH_m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2 = m1_beta_mu_j_next_keyBy_h2.cogroup(m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2)
        m1_beta_mu_j_draw.unpersist()
        m1_beta_mu_j_draw_next = Joined_m1_beta_mu_j_next_keyBy_h2_WITH_m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2.map(gtr.get_beta_draw, preservesPartitioning = True).persist()
        #print "count  m1_beta_mu_j_draw_next   ", m1_beta_mu_j_draw_next.count()
        #print "take 1 m1_beta_mu_j_draw_next ", m1_beta_mu_j_draw_next.take(1)
        #OPTIMIZATION : NO NEED for unions m1_beta_mu_j_draw = m1_beta_mu_j_draw.union(m1_beta_mu_j_draw_next)
        # beta_mu_j_draw keyed by h2
        m1_beta_mu_j_draw = m1_beta_mu_j_draw.keyBy(lambda (iter, hierarchy_level2, beta_mu_j_draw): hierarchy_level2)
        
        # Update values of s2
        ##-- Compute updated value of s2 to use in next section.
        print "Updating values of s2"
        m1_beta_i_draw_group_by_h2_h1 = m1_beta_i_draw.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, beta_i_draw): (hierarchy_level2, hierarchy_level1))
        # The structure m1_beta_i_draw_next doesnt change during iterations. 
        # m1_beta_i_draw_next is already computed in Gibbs_init via : m1_d_array_agg_key_by_h2_h1 = m1_d_array_agg.keyBy(lambda (keys, x_matrix, y_array, hierarchy_level2, hierarchy_level1) : (keys[0], keys[1]))
        JOINED_m1_beta_i_draw_WITH_m1_d_array_agg = m1_d_array_agg_key_by_h2_h1.cogroup(m1_beta_i_draw_group_by_h2_h1)
        #print "JOINED_m1_beta_i_draw_WITH_m1_d_array_agg : 2 : ", JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.take(1)
        # hierarchy_level2, hierarchy_level1, x_array_var, y_var, iter, beta_i_draw
        JOINED_m1_beta_i_draw_WITH_m1_d_array_agg = JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.map(lambda (x, y): (x[0], x[1], list(y[0])[0][1], list(y[0])[0][2] ,list(y[1])[0][0], list(y[1])[0][3], m1_d_count_grpby_level2_b.value[x[0]]))
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
        m1_s2_next = foo2.groupByKey().map(lambda (x, y): gtr.get_s2(list(y)))
        # OPTIMIZATION no need for union witht he previous step as it is not required in further iterations 
        # it is computed new each time.
        ##?}>>>m1_s2 = m1_s2.union(m1_s2_next)
        #print "m1_s2 : ", m1_s2.take(1)
        #print "m1_s2 : ", m1_s2.count()
        
        ## Updating values of h_draw based on current iteration
        # -- Draw h from gamma dist'n.  Note that h=1/(s^2)
        ## from iteri, hierarchy_level2, m1_d_count_grpby_level2_b, s2
        ## m1_h_draw = iteri, h2, h_draw
        m1_h_draw_next = m1_s2_next.map(gtr.get_h_draw).keyBy(lambda (iteri, h2, h_draw): h2).persist()
        # optimization we dont need to persist the previous draws with new ones 
        ## so removing the persistence and creating new persistence
        m1_h_draw = m1_h_draw.union(m1_h_draw_next)
        print "m1_h_draw : ", m1_h_draw.take(1)
        print "m1_h_draw : ", m1_h_draw.count()
        
        ## Creating vertical draws
        ## -- Convert the array-based draws from the Gibbs Sampler into a "vertically long" format by unnesting the arrays.
        # lists of tuples : s, h2, h1, beta_i_draw[:,i][0], driver_x_array[i], hierarchy_level2_hierarchy_level1_driver        
        m1_beta_i_draw_long_next = m1_beta_i_draw.map(gtr.get_beta_i_draw_long).reduce(add)
        print "reduce count ", len(m1_beta_i_draw_long_next)
        print "reduce ", m1_beta_i_draw_long_next[0]
        print "reduce ", m1_beta_i_draw_long_next[1]
        if 'm1_beta_i_draw_long' in locals():
            m1_beta_i_draw_long = m1_beta_i_draw_long + m1_beta_i_draw_long_next
        else:
            m1_beta_i_draw_long = m1_beta_i_draw_long_next
     
    # structured as (h2, h1, driver) -> (s, h2, h1, beta_draw[i], x_array[i], h2_h1_driver)    
    m1_beta_i_draw_long = sc.parallelize(m1_beta_i_draw_long)
    #.keyBy(lambda (s, h2, h1, beta_i_draw, driver, h2_h1_driver): (h2, h1, driver))
    print "m1_beta_i_draw_long count :", m1_beta_i_draw_long.count()
    print "m1_beta_i_draw_long take :", m1_beta_i_draw_long.take(1)
        
    print gibbs_iteration_text()
    
    return (m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount, m1_d_count_grpby_level2 ,m1_h_draw  ,m1_s2 ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_j_mu, m1_beta_i_draw_long)
