# -*- coding: utf-8 -*-
"""
@author: ssoni
"""

import gibbs_udfs as gu
import gibbs_transformations as gtr

def gibbs_iteration_text():
    text_output = 'Done: All requested Gibbs Sampler updates are complete.  All objects associated with this model are named with a m1 prefix.'   
    return text_output

def add(x,y):
    return (x+y)

#                  d, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, begin_iter, end_iter
def gibbs_iter(sc, begin_iter, end_iter, m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw ,m1_s2 ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_j_mu):
    
    m1_beta_mu_j_draw_keyBy_h2 = m1_beta_mu_j_draw.keyBy(lambda (iter, hierarchy_level2, beta_mu_j_draw): hierarchy_level2).cache()
    m1_d_array_agg_key_by_h2_h1 = m1_d_array_agg.keyBy(lambda (keys, x_matrix, y_array, hierarchy_level2, hierarchy_level1) : (keys[0], keys[1])).cache() 
    m1_d_count_grpby_level2_b = sc.broadcast(m1_d_count_grpby_level2).cache()
    m1_d_array_agg_constants_key_by_h2_h1 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2, h1)).cache()
    m1_d_array_agg_constants_key_by_h2 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2)).cache()
    m1_d_childcount_groupBy_h2 = m1_d_childcount.keyBy(lambda (hierarchy_level2, n1) : hierarchy_level2).cache()
    
    for s in range(begin_iter, end_iter+1):
        
        ## Inserting into m1_beta_i
        print "Inserting into m1_beta_i"
        m1_h_draw_filter_by_iteration = m1_h_draw.filter(lambda (iteri, h2, h_draw): iteri == s - 1 ).keyBy(lambda (iteri, h2, h_draw): h2).cache()
        #print "m1_h_draw_filter_by_iteration", m1_h_draw_filter_by_iteration.collect()
        m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration = m1_Vbeta_inv_Sigmabeta_j_draw.filter(lambda (iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): iteri == s - 1 ).keyBy(lambda (iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2)
        # filter m1_h_draw taking only |s| -1 into account
        m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration_join_m1_h_draw_filter_by_iteration = m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration.cogroup(m1_h_draw_filter_by_iteration)
        
        # Creating a new structure joined_simplified : iteri, h2, Vbeta_inv_j_draw, h_draw, which is a simplified version of what the original structure looks.
        joined_simplified = m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration_join_m1_h_draw_filter_by_iteration.map(lambda (x,y): (list(y[0])[0][0], x, list(y[0])[0][3], list(y[1])[0][2]))
        joined_simplified_key_by_h2 = joined_simplified.keyBy(lambda (iteri, h2, Vbeta_inv_j_draw, h_draw): h2)
        ## m1_d_array_agg_constants is RDD of tuples h2, h1, xtx, xty
        ## joined_simplified is RDD of tuples h2 -> iteri, h2, Vbeta_inv_j_draw, h_draw 
        # join the m1_d_array_agg_constants_key_by_h2 with join_simplified
        m1_d_array_agg_constants_key_by_h2_join_joined_simplified = m1_d_array_agg_constants_key_by_h2.cogroup(joined_simplified_key_by_h2)
        #print "count and take 1", m1_d_array_agg_constants_key_by_h2_join_joined_simplified.count(), m1_d_array_agg_constants_key_by_h2_join_joined_simplified.take(1)
        # following two lines of mapped RDD are for testing : m1_d_array_agg_constants_key_by_h2_join_joined_simplified
        #m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped = m1_d_array_agg_constants_key_by_h2_join_joined_simplified.map(lambda (x,y): (x, list(y[0])[0], list(y[1])[0]))
        #print "m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped", m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped.take(1)
        
        # compute next values of m1_Vbeta_i : (h2, [(count, hierarchy_level2, hierarchy_level1, Vbeta_i)])
        m1_Vbeta_i_keyBy_h2_long_next = m1_d_array_agg_constants_key_by_h2_join_joined_simplified.map(lambda (x,y): (x, gtr.get_Vbeta_i_next(y, s)))
        # the Unified table is the actual table that reflects all the rows of m1_Vbeta_i in correct format.
        m1_Vbeta_i = m1_Vbeta_i.union(sc.parallelize(m1_Vbeta_i_keyBy_h2_long_next.values().reduce(add)))
        #print "count  m1_Vbeta_i_unified   ", m1_Vbeta_i_unified.count()
        #print "take 1 m1_Vbeta_i_unified ", m1_Vbeta_i_unified.take(1)
        m1_Vbeta_i_keyby_h2_h1 = m1_Vbeta_i.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i): (hierarchy_level2, hierarchy_level1))
    
       
        ### Inserting into beta_i_mean
        print "Inserting into beta_i_mean"
        m1_Vbeta_i_unified_filter_iter_s = m1_Vbeta_i.filter(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i) : i == s)
        m1_Vbeta_i_keyby_h2_h1 = m1_Vbeta_i_unified_filter_iter_s.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i): (hierarchy_level2, hierarchy_level1))
        #print "count  m1_Vbeta_i_keyby_h2_h1   ", m1_Vbeta_i_keyby_h2_h1.count()
        #print "take 1 m1_Vbeta_i_keyby_h2_h1 ", m1_Vbeta_i_keyby_h2_h1.take(1)    
        #  JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1 of tuples : hierarchy_level2, hierarchy_level1, Vbeta_i,xty 
        # following is the foo of computer beta_i_mean
        JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1 = m1_Vbeta_i_keyby_h2_h1.cogroup(m1_d_array_agg_constants_key_by_h2_h1).map(lambda (x,y): (list(y[0])[0][1],list(y[0])[0][2], list(y[0])[0][3], list(y[1])[0][3]))
        JOINED_part_1_by_keyBy_h2 = JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1.keyBy(lambda (hierarchy_level2, hierarchy_level1, Vbeta_i, xty): hierarchy_level2)
        
        # m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration joined with m1_beta_mu_j_draw_by_previous_iteration
        # m1_beta_mu_j_draw = hierarchy_level2 -> (iter, hierarchy_level2, beta_mu_j_draw)
        m1_beta_mu_j_draw_by_previous_iteration = m1_beta_mu_j_draw_keyBy_h2.filter(lambda (x,y): y[0] == s - 1)
        #print "count  m1_beta_mu_j_draw_by_previous_iteration   ", m1_beta_mu_j_draw_by_previous_iteration.count()
        #print "take 1 m1_beta_mu_j_draw_by_previous_iteration ", m1_beta_mu_j_draw_by_previous_iteration.take(1)
        # Following is computed from h2 ->(resIter1, resIter2) <=> where 
        # resIter1 is (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j)
        # resIter2 is (iter, hierarchy_level2, beta_mu_j_draw)
        # strucuture is  ( h2, iter, Vbeta_inv_j_draw, beta_mu_j_draw )
        JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw = m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration.cogroup(m1_beta_mu_j_draw_by_previous_iteration).map(lambda (x,y): (x, list(y[0])[0][0], list(y[0])[0][3], list(y[1])[0][2])).keyBy(lambda (hierarchy_level2, iteri, Vbeta_inv_j_draw, beta_mu_j_draw): hierarchy_level2)
        # Cogroup above structure h2 -> h2, iter, Vbeta_inv_j_draw, beta_mu_j_draw with m1_h_draw_filter_by_iteration,  h2 -> (iteri, h2, h_draw)
        JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw_join_WITH_h_draw = JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.cogroup(m1_h_draw_filter_by_iteration).map(lambda (x, y): (list(y[0])[0][0], list(y[1])[0][2], list(y[0])[0][2], list(y[0])[0][3]))
        JOINED_part_2_by_keyBy_h2 = JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw_join_WITH_h_draw.keyBy(lambda (hierarchy_level2, h_draw, Vbeta_inv_j_draw, beta_mu_j_draw): hierarchy_level2)
        #print "take 1 ", JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw_join_WITH_h_draw.take(1) 
        #print "count 1 ", JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw_join_WITH_h_draw.count()
        m1_beta_i_mean_keyBy_h2_long_next = JOINED_part_1_by_keyBy_h2.cogroup(JOINED_part_2_by_keyBy_h2).map(lambda (x,y): (x, gtr.get_beta_i_mean_next(y, s)))
        # the Unified table is the actual table that reflects all rows of m1_beta_i_draw in correct format.
        # strucutured as iter or s, h2, h1, beta_i_mean
        m1_beta_i_mean = m1_beta_i_mean.union(sc.parallelize(m1_beta_i_mean_keyBy_h2_long_next.values().reduce(add)))
        #print "count  m1_Vbeta_i_unified   ", m1_beta_i_draw_unified.count()
        #print "take 1 m1_Vbeta_i_unified ", m1_beta_i_draw_unified.take(1)
        m1_beta_i_mean_keyBy_h2_h1 = m1_beta_i_mean.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, beta_i_mean): (hierarchy_level2, hierarchy_level1))
        
        # insert into beta_i, using iter=s-1 values.  Draw from mvnorm dist'n.
        print "insert into beta_i"
        # for strucuter like iter, h2, h1, beta_draw,
        # ,for iter for iter == s, join beta_i_mean and Vbeta_i for structure s, h2, h1, beta_i_mean, Vbeta_i
        m1_beta_i_mean_by_current_iteration = m1_beta_i_mean_keyBy_h2_h1.filter(lambda (x,y): y[0] == s)
        m1_Vbeta_i_by_current_iteration = m1_Vbeta_i_keyby_h2_h1.filter(lambda (x,y): y[0] == s)
        # After cogroup of above two Data Structures we can easily compute bet_draw directly from the map function
        # structure : s, h2, h1, beta_draw 
        m1_beta_i_draw_next = m1_beta_i_mean_by_current_iteration.cogroup(m1_Vbeta_i_by_current_iteration).map(lambda (x,y): (s, x[0], x[1], gu.beta_draw(list(y[0])[0][3], list(y[1])[0][3])))
        #print "count  m1_beta_i_draw_next   ", m1_beta_i_draw_next.count()
        #print "take 1 m1_beta_i_draw_next ", m1_beta_i_draw_next.take(1)
        m1_beta_i_draw = m1_beta_i_draw.union(m1_beta_i_draw_next)
        #print "count  m1_beta_i_draw   ", m1_beta_i_draw.count()
        #print "take 1 m1_beta_i_draw ", m1_beta_i_draw.take(1)
        
        # insert into Vbeta_j_mu table 
        print "Inserting into Vbeta_j_mu"
        # using using the most "recent" values of the beta_i_draw coefficients
        # The values of beta_mu_j_draw and Vbeta_j_mu have not yet been updated at this stage, so their values at iter=s-1 are taken.
        # S1 : h2, h1, beta_i_draw  from m1_beta_i_draw where iteri == s ==> m1_beta_i_draw_next => key it by h2
        m1_beta_i_draw_next_key_by_h2 = m1_beta_i_draw_next.keyBy(lambda (s, h2, h1, beta_i_draw): h2)
        # S2 : h2, beta_mu_j_draw from m1_beta_mu_j_draw where iter= s-1 and also key it by h2
        # m1_beta_mu_j_draw_by_previous_iteration = hierarchy_level2 -> (s-1, hierarchy_level2, beta_mu_j_draw)
        JOINED_m1_beta_i_draw_next_key_by_h2_WITH_m1_beta_mu_j_draw_by_previous_iteration = m1_beta_i_draw_next_key_by_h2.cogroup(m1_beta_mu_j_draw_by_previous_iteration)
        m1_Vbeta_j_mu_next = JOINED_m1_beta_i_draw_next_key_by_h2_WITH_m1_beta_mu_j_draw_by_previous_iteration.map(lambda (h2,y): (s, h2, gtr.get_Vbeta_j_mu_next(y)))
        #print "count  m1_Vbeta_j_mu_next   ", m1_Vbeta_j_mu_next.count()
        #print "take 1 m1_Vbeta_j_mu_next ", m1_Vbeta_j_mu_next.take(1)
        m1_Vbeta_j_mu = m1_Vbeta_j_mu.union(m1_Vbeta_j_mu_next)
        #print "count  m1_Vbeta_j_mu   ", m1_Vbeta_j_mu.count()
        #print "take 1 m1_Vbeta_j_mu ", m1_Vbeta_j_mu.take(1)
        
        ## inserting into m1_Vbeta_inv_Sigmabeta_j_draw
        print "inserting into m1_Vbeta_inv_Sigmabeta_j_draw"
        ## computing Vbeta_inv_j_draw from m1_Vbeta_j_mu where iteration == s
        ## returns s, h2, Vbeta_inv_j_draw
        m1_Vbeta_j_mu_pinv = m1_Vbeta_j_mu_next.map(gtr.get_m1_Vbeta_j_mu_pinv).keyBy(lambda (s, h2, Vbeta_inv_j_draw): h2)
        #print "count  m1_Vbeta_j_mu_pinv   ", m1_Vbeta_j_mu_pinv.count()
        #print "take 1 m1_Vbeta_j_mu_pinv ", m1_Vbeta_j_mu_pinv.take(1)
        
        # structure m1_d_childcount_groupBy_h2 can be used,
        # m1_d_childcount_groupBy_h2 has a structure h2 -> h2, n1
        JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2 = m1_Vbeta_j_mu_pinv.cogroup(m1_d_childcount_groupBy_h2)
        # s, h2, Vbeta_inv_j_draw, n1
        JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified = JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2.map(lambda (x,y): (list(y[0])[0][0], list(y[0])[0][1], list(y[0])[0][2], list(y[1])[0][1]))
        #print "count  JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified   ", JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified.count()
        #print "take 1 JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified ", JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified.take(1)    
        # iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j
        m1_Vbeta_inv_Sigmabeta_j_draw_next = JOINED_m1_Vbeta_j_mu_pinv_WITH_m1_d_childcount_groupBy_h2_simplified.map(gtr.get_m1_Vbeta_inv_Sigmabeta_j_draw_next)
        #print "count  m1_Vbeta_inv_Sigmabeta_j_draw_next   ", m1_Vbeta_inv_Sigmabeta_j_draw_next.count()
        #print "take 1 m1_Vbeta_inv_Sigmabeta_j_draw_next ", m1_Vbeta_inv_Sigmabeta_j_draw_next.take(1)
        ## appending the next iteration values to previous Data Structure
        m1_Vbeta_inv_Sigmabeta_j_draw = m1_Vbeta_inv_Sigmabeta_j_draw.union(m1_Vbeta_inv_Sigmabeta_j_draw_next)
        
        ## inserting into m1_beta_mu_j
        print "Inserting into m1_beta_mu_j"
        # -- Compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  
        # -- Get back one coefficient vector for each j (i.e. J  coefficient vectors are returned).
        # first we modify m1_beta_i_draw to compute sum_coef_j
        # m1_beta_i_draw_keyby_h2 has a key-value structure h2 -> (s, h2, h1, beta_i_draw) into h2 -> h2, sum_coef_j
        m1_beta_i_draw_next_key_by_h2_sum_coef_j = m1_beta_i_draw_next_key_by_h2.map(lambda (x,y): (x, y[3])).keyBy(lambda (h2, coeff): h2).groupByKey().map(lambda (key, value) : gtr.add_coeff_j(key,value))
        # NEXT 
        # Join m1_Vbeta_inv_Sigmabeta_j_draw (with iteration == s, i.e., m1_Vbeta_inv_Sigmabeta_j_draw_next, select *) 
        # & m1_beta_i_draw (with iteration == s, i.e., m1_beta_i_draw_next_key_by_h2_sum_coef_j, select h2, sum_coef_j)   
        m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2 = m1_Vbeta_inv_Sigmabeta_j_draw_next.keyBy(lambda (s, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2)
        Joined_m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2_WITH_m1_beta_i_draw_next_key_by_h2_sum_coef_j = m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2.cogroup(m1_beta_i_draw_next_key_by_h2_sum_coef_j)
        # Using same function as was used in the gibbs_init
        # s, h2, beta_mu_j
        m1_beta_mu_j_next = Joined_m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2_WITH_m1_beta_i_draw_next_key_by_h2_sum_coef_j.map(gtr.get_substructure_beta_mu_j)
        m1_beta_mu_j = m1_beta_mu_j.union(m1_beta_mu_j_next)
        #print "count  m1_beta_mu_j_next   ", m1_beta_mu_j_next.count()
        #print "take 1 m1_beta_mu_j_next ", m1_beta_mu_j_next.take(1)
        # h2 -> s, h2, beta_mu_j
        # Beta_mu_j keyed by h2
        # m1_beta_mu_j_keyBy_h2 = m1_beta_mu_j.keyBy(lambda (iter, hierarchy_level2, beta_mu_j): hierarchy_level2)
        
        # inserting into m1_beta_mu_j_draw
        # -- Draw beta_mu from mvnorm dist'n.  Get back J vectors of beta_mu, one for each J.  Note that all input values are at iter=s.
        print "inserting into m1_beta_mu_j_draw"
        # Structures : m1_beta_mu_j and m1_Vbeta_inv_Sigmabeta_j_draw for current iteration will be cogrouped.
        # m1_beta_mu_j_next : s, h2, beta_mu_j, we will need h2, beta_mu_j
        # m1_Vbeta_inv_Sigmabeta_j_draw_next : s, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j, we will need h2, Sigmabeta_j
        m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2 = m1_Vbeta_inv_Sigmabeta_j_draw_next.keyBy(lambda (s, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2)
        m1_beta_mu_j_next_keyBy_h2 = m1_beta_mu_j_next.keyBy(lambda (s, h2, beta_mu_j): h2)
        # now the cogroup
        Joined_m1_beta_mu_j_next_keyBy_h2_WITH_m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2 = m1_beta_mu_j_next_keyBy_h2.cogroup(m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2)
        m1_beta_mu_j_draw_next = Joined_m1_beta_mu_j_next_keyBy_h2_WITH_m1_Vbeta_inv_Sigmabeta_j_draw_next_keyBy_h2.map(gtr.get_beta_draw)
        #print "count  m1_beta_mu_j_draw_next   ", m1_beta_mu_j_draw_next.count()
        #print "take 1 m1_beta_mu_j_draw_next ", m1_beta_mu_j_draw_next.take(1)
        m1_beta_mu_j_draw = m1_beta_mu_j_draw_next.union(m1_beta_mu_j_draw_next)
        # beta_mu_j_draw keyed by h2
        m1_beta_mu_j_draw_keyBy_h2 = m1_beta_mu_j_draw.keyBy(lambda (iter, hierarchy_level2, beta_mu_j_draw): hierarchy_level2)
        
        # Update values of s2
        ##-- Compute updated value of s2 to use in next section.
        print "Updating values of s2"
        m1_beta_i_draw_group_by_h2_h1 = m1_beta_i_draw_next.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, beta_i_draw): (hierarchy_level2, hierarchy_level1))
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
        m1_s2 = m1_s2.union(m1_s2_next)
        print "m1_s2 : ", m1_s2.take(1)
        print "m1_s2 : ", m1_s2.count()
        
        ## Updating values of h_draw based on current iteration
        # -- Draw h from gamma dist'n.  Note that h=1/(s^2)
        ## from iteri, hierarchy_level2, m1_d_count_grpby_level2_b, s2
        ## m1_h_draw = iteri, h2, h_draw
        m1_h_draw_next = m1_s2_next.map(gtr.get_h_draw)
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
