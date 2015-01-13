# -*- coding: utf-8 -*-
"""
@author: ssoni
"""

import numpy as np
from pyspark import SparkContext
import sys
import gibbs_udfs as gu
import nearPD as npd

#                  d, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, begin_iter, end_iter
def gibbs_test(sc, d, hierarchy_level1, hierarchy_level2, p, df1, y_var, x_var_array, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, begin_iter, end_iter, keyBy_groupby_h2_h1):
    
    counter = begin_iter     
    while counter <= end_iter:
        counter += 1
        print "iter", counter
        # call the gibbs final implementations
    
    # s = begin_iter
    s = 2
    
    ## -- Compute Vbeta_i
    # filter first then do a key by as it will reduce the data for shuffle during keyby
    m1_h_draw = sc.textFile("hdfs://m1_h_draw.txt")
    m1_h_draw_filter_by_iteration = m1_h_draw.filter(lambda (iteri, h2, h_draw): iteri == s - 1 ).keyBy(lambda (iteri, h2, h_draw): h2)
    # filter # m1_Vbeta_inv_Sigmabeta_j_draw : (iter, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j) filter by |s| -1 into account
    m1_Vbeta_inv_Sigmabeta_j_draw = sc.textFile("hdfs://m1_Vbeta_inv_Sigmabeta_j_draw.txt")
    m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration = sc.parallelize(m1_Vbeta_inv_Sigmabeta_j_draw).filter(lambda (iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): iteri == s - 1 ).keyBy(lambda (iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2)
    # filter m1_h_draw taking only |s| -1 into account
    m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration_join_m1_h_draw_filter_by_iteration = m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration.cogroup(m1_h_draw_filter_by_iteration)
    # joined_simplified : iteri, h2, Vbeta_inv_j_draw, h_draw
    joined_simplified = m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration_join_m1_h_draw_filter_by_iteration.map(lambda (x,y): (list(y[0])[0][0], x, list(y[0])[0][3], list(y[1])[0][2]))
    joined_simplified_key_by_h2 = joined_simplified.keyBy(lambda (iteri, h2, Vbeta_inv_j_draw, h_draw): h2)
    print "simplified ", joined_simplified_key_by_h2.collect()
    ## m1_d_array_agg_constants is RDD of tuples h2, h1, xtx, xty
    ## joined_simplified is RDD of tuples h2 -> iteri, h2, Vbeta_inv_j_draw, h_draw 
    # join the m1_d_array_agg_constants_key_by_h2 with join_simplified
    m1_d_array_agg_constants = sc.textFile("hdfs://m1_d_array_agg_constants.txt")
    m1_d_array_agg_constants_key_by_h2 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2))
    
    m1_d_array_agg_constants_key_by_h2_join_joined_simplified = m1_d_array_agg_constants_key_by_h2.cogroup(joined_simplified_key_by_h2)
    print "count and take 1", m1_d_array_agg_constants_key_by_h2_join_joined_simplified.count(), m1_d_array_agg_constants_key_by_h2_join_joined_simplified.take(1)
    m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped = m1_d_array_agg_constants_key_by_h2_join_joined_simplified.map(lambda (x,y): (x, list(y[0])[0], list(y[1])[0]))
    print "m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped", m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped.take(1)
    # compute next values of m1_Vbeta_i
    m1_Vbeta_i_next = m1_d_array_agg_constants_key_by_h2_join_joined_simplified.map(lambda (x,y): (x, get_Vbeta_i_next(y, s)))
    m1_Vbeta_i = m1_Vbeta_i.union(m1_Vbeta_i_next) 
    print "count m1_Vbeta_i", m1_Vbeta_i.count()
    print "take 1 m1_Vbeta_i", m1_Vbeta_i.take(1)
    
    ## -- Compute beta_i_mean
