"""

@author: ssoni
"""
import gibbs_udfs as gu
import gibbs_transformations as gtr
import gibbs_partitions as gp

def gibbs_init_text():
    text_output = 'Done: Gibbs Sampler for model m1 is initialized.  Proceed to run updates of the sampler by using the gibbs() function.  All objects associated with this model are named with a m1 prefix.'
    return text_output

def add(x,y):
    return (x+y)

# Initialize Gibbs with initial values for future iterations
# Pre-computing quantities that are contant throughout sampler iterations
# Call as gi.gibbs_init_test(sc, d, keyBy_groupby_h2_h1, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals)
def gibbs_initializer(sc, d, h1_h2_partitions,h2_partitions, d_key_h2, d_key_h2_h1, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals):
    
    # For Detailed Explanation
    # Create array aggregated version of d.  
    # For the response variable, collapse data from all weeks into a single array named y for each department_name-tier combo.  
    # For the set of explanatory variables, in our case 14 explanatory variables, we collapse data from all weeks into a single array named x_matrix.  
    # The data type of x_matrix is a 2-dimensional array.  
    # There is one cell of x_matrix for each department_name-tier combo, and 
    # the dimension for each x_matrix is (# of weeks of data in the department_name-tier)x(# of explanatory variables), 
    # which equals (# of weeks of data in the department_name-tier)x(14) since we have 14 explanatory variables.  
    # We end up with a Data Structure d_array_agg with as many rows as the number of distinct department_name-tier combos.  
    # With the original data (with 14162 data points) of we end up with 135 records.  
    # m1_d_array_agg : tuples of ( keys, x_matrix, y_array, hierarchy_level2[1,0], hierarchy_level1[1,0] )
    #m1_d_array_agg = keyBy_groupby_h2_h1.map(gtr.create_x_matrix_y_array).cache()
    
    # OPTIMIZATION 3 : create m1_d_arry_agg values on each partitioned block of data which is keyed by h2 h1 
    #m1_d_array_agg = d_key_h2_h1.groupByKey().map(gtr.create_x_matrix_y_array)
    #### OR h2,h1, x_matrix, y_array
    d_groupedBy_h1_h2 = d.groupBy(gp.group_partitionByh2h1, h1_h2_partitions).persist()
    m1_d_array_agg = d_groupedBy_h1_h2.map(gtr.create_x_matrix_y_array, preservesPartitioning=True).persist()
    
    #  Compute constants of X'X and X'y for computing 
    #  m1_Vbeta_i & beta_i_mean
    #  m1_d_array_agg_constants : list of tuples of (h2, h1, xtx, xty)
    # OPTIMIZATION preserving the values on partitions
    m1_d_array_agg_constants = m1_d_array_agg.map(gtr.create_xtx_matrix_xty, preservesPartitioning=True).persist()
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
    m1_d_childcount = gtr.get_d_childcount(d)
    m1_d_childcount_b = sc.broadcast(m1_d_childcount.collect())
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
    m1_d_count_grpby_level2_b = sc.broadcast(gtr.get_d_count_grpby_level2(d).countByKey())
    
    # Arrange d keyBy h2 or hierarchy_level2
    #d_keyBy_h2 = d.keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2)).groupByKey().map(gtr.create_x_matrix_y_array)
    # OPTIMIZATION d_keyBy_h2 to use similar concept as we sued ot build m1_d_array_agg
    # OPTIMIZATION 2 : very simple task of computing a 5 count array so we would rather not persist it this time.
    d_groupedBy_h2 = d.groupBy(gp.group_partitionByh2, h2_partitions).persist()
    d_keyBy_h2 = d_groupedBy_h2.map(gtr.create_x_matrix_y_array, preservesPartitioning=True)
    
    # Compute OLS estimates for reference    
    if(initial_vals == "ols"):
        # Initial values of coefficients for each department_name-tier (i).  Set crudely as OLS regression coefficients for each department_name-tier (i).
        # OPTIMIZATION SINCE we started doing group by partitionings its not so clear as to still have keyBy h2 h1 or not, Omitting it, till its needed.
        # m1_ols_beta_i = m1_d_array_agg.map(gtr.get_ols_initialvals_beta_i, preservesPartitioning=True).keyBy(lambda (h2,h1,coff): (h2, h1))
        # h2, h1, ols_beta_i
        # OPTIMIZATION instead of preserving partitioning here, I keep it at the h2 level, so that we have a chance to 
        m1_ols_beta_i = m1_d_array_agg.map(gtr.get_ols_initialvals_beta_i, preservesPartitioning=True).persist()
        # print "Coefficients for LinearRegression ", m1_ols_beta_i.count()
        
        # Initial values of coefficients for each department_name (j).  Set crudely as OLS regression coefficients for each department_name (j).
        # Similarly we compute the m1_ols_beta_j which uses the RDD mapped upon only hierarchy_level2
        # OPTIMIZATION SINCE we started doing group by partitionings its not so clear as to still have keyBy h2 h1 or not, Omitting it, till its needed.
        # m1_ols_beta_j = d_keyBy_h2.map(gtr.get_ols_initialvals_beta_j).keyBy(lambda (h2,coff): (h2))
        # OPTIMIZATION 2 , lets collect it and then we can transfer it to various nodes where the m1_ols_beta_i resides
        # h2, coeff
        # m1_ols_beta_j.keys().collect() : [u'"5"', u'"1"', u'"2"', u'"3"', u'"4"']
        #m1_ols_beta_j = d_keyBy_h2.map(gtr.get_ols_initialvals_beta_j, preservesPartitioning=True).persist()
        # OPTIMIZATION 3, actually creating a collection of this small data set and preserving it with driver or
        # boradcasting it so as to have a highly distributed m1_ols_beta_i's stationary into their partitions 
        # and save shuffle costs
        m1_ols_beta_j_collection = d_keyBy_h2.map(gtr.get_ols_initialvals_beta_j).collect()

        # print "Coefficients for LinearRegression after keyby H2", m1_ols_beta_j.count()
        
    # In case the initial_vals are defined as "random" we compute the coefficients for each department_name-tier (i) and for each department_name (j)
    # We compute these coefficients using deviates from Uniform distribution
    if(initial_vals == "random"):
        # print "Draw random array samples of p elements from the uniform(-1,1) dist'n"
        # OPTIMIZATION SINCE we started doing group by partitionings its not so clear as to still have keyBy h2 h1 or not, Omitting it, till its needed.
        #m1_ols_beta_i = m1_d_array_agg.map(gtr.get_random_initialvals_beta_i).keyBy(lambda (h2,h1,coff): (h2, h1))
        #m1_ols_beta_j = d_keyBy_h2.map(gtr.get_random_initialvals_beta_j).keyBy(lambda (h2,coff): (h2))
        m1_ols_beta_i = m1_d_array_agg.map(gtr.get_random_initialvals_beta_i, preservesPartitioning=True).persist()
        #m1_ols_beta_j = d_keyBy_h2.map(gtr.get_random_initialvals_beta_j, preservesPartitioning=True)
        m1_ols_beta_j_collection = d_keyBy_h2.map(gtr.get_random_initialvals_beta_j).collect()
        
    
    #-- Compute m1_Vbeta_j_mu 
    #   Using the above initial values of the coefficients and drawn values of priors,
    #   compute initial value of coefficient var-cov matrix (Vbeta_j_mu)
    #   FOR EACH group i, with group j coefficients as priors, and
    #   then sum them to get back Vbeta_j_mu matrices
    # computing _Vbeta_j_mu  
    joined_i_j = gtr.create_join_by_h2_only(m1_ols_beta_i.collect(), m1_ols_beta_j.collect())
    # keyBy and groupBy will reduce the rows from 135 to 5 since there are only 5 hierarchy_level2's
    # joined_i_j_rdd.take(1) :  (u'"5"', <pyspark.resultiterable.ResultIterable object at 0x117be50>) similarly 5 others
    joined_i_j_rdd = sc.parallelize(joined_i_j).keyBy(lambda (hierarchy_level2, hierarchy_level1, values_array_i, values_array_j): (hierarchy_level2)).groupByKey()
    ## Data Structure m1_Vbeta_j_mu is symmetric along diagonal and have same dimensions as the one in HAWQ tables.
    # print "coefficients i and j", joined_i_j_rdd.take(1)
    m1_Vbeta_j_mu = joined_i_j_rdd.map(lambda (x, y): (1, x, gtr.get_Vbeta_j_mu(y))) 
    # print " m1_Vbeta_j_mu count ", m1_Vbeta_j_mu.count() 
    # print " m1_Vbeta_j_mu take 1", m1_Vbeta_j_mu.take(1)
    
    ###-- Draw Vbeta_inv and compute resulting sigmabeta using the above functions for each j    
    m1_Vbeta_j_mu_pinv = m1_Vbeta_j_mu.map(gtr.get_m1_Vbeta_j_mu_pinv).keyBy(lambda (seq, hierarchy_level2, Vbeta_inv_j_draw) : (hierarchy_level2)).groupByKey() 
    m1_d_childcount_groupBy_h2 = m1_d_childcount.keyBy(lambda (hierarchy_level2, n1) : hierarchy_level2).groupByKey()
    #  print "m1_d_childcount_groupBy_h2 ", m1_d_childcount_groupBy_h2.collect()
    #  here vals are iter, h2,
    #  y[0][0] = iter or seq from m1_Vbeta_j_mu_pinv
    #  y[0][1] = h2 from in m1_Vbeta_j_mu_pinv
    #  y[1][1] = n1 from m1_d_childcount_groupBy_h2,
    #  y[0][2] = Vbeta_inv_j_draw from m1_Vbeta_j_mu_pinv, pinv_Vbeta_inv_Sigmabeta_j_draw()
    #  error 'ResultIterable' object does not support indexing
    #  map(lambda (x,y): (x, sum(fun(list(y)))), joined_i_j_rdd.take(1))
    joined_Vbeta_i_j = sorted(m1_Vbeta_j_mu_pinv.cogroup(m1_d_childcount_groupBy_h2).collect())
    # print " cogroup counts: ", len(joined_Vbeta_i_j)
    # print " Vbeta_i_j cogroup take 1", joined_Vbeta_i_j[1]
    # print "map ", map(lambda (x,y): (x, (y for y in list(y[0]))), joined_Vbeta_i_j)
    # m1_Vbeta_inv_Sigmabeta_j_draw : iter, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j
    m1_Vbeta_inv_Sigmabeta_j_draw = sc.parallelize(map(lambda (x,y): gtr.get_m1_Vbeta_inv_Sigmabeta_j_draw(list(y)), joined_Vbeta_i_j)) 
    # print " m1_Vbeta_inv_Sigmabeta_j_draw Take 1: ", m1_Vbeta_inv_Sigmabeta_j_draw[1]
    # print " m1_Vbeta_inv_Sigmabeta_j_draw Count: ", len(m1_Vbeta_inv_Sigmabeta_j_draw)
    m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2 = m1_Vbeta_inv_Sigmabeta_j_draw.keyBy(lambda (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j): (hierarchy_level2)) 
    
    ##-- m1_beta_mu_j : Compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  
    ##-- Get back one coefficient vector for each j (i.e. J  coefficient vectors are returned).
    ## computing _beta_mu_j
    ## for computing _beta_mu_j we first will modify m1_ols_beta_i or _initialvals_beta_i to get sum_coef_j 
    ## and then  we will join it with m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2
    #  modifying m1_ols_beta_i_sum_coef_j (h2, h1) -> (h2,h1,coff) 
    m1_ols_beta_i_sum_coef_j = m1_ols_beta_i.map(lambda (x,y): (x[0], y[2])).keyBy(lambda (h2, coeff): h2).groupByKey().map(lambda (key, value) : gtr.add_coeff_j(key,value))
    # print "Diagnostics m1_ols_beta_i_sum_coef_j ", m1_ols_beta_i_sum_coef_j.collect()    
    joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j = m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2.cogroup(m1_ols_beta_i_sum_coef_j)
    # print "joined_m1_Vbeta_inv ", joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j.take(1)  
    # m1_beta_mu_j is  RDD keyed structure as (iter, hierarchy_level2, beta_mu_j)
    # beta_mu_j is mean with dim 13 X 1
    m1_beta_mu_j = joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j.map(gtr.get_substructure_beta_mu_j)
    # hierarchy_level2=> (iter, hierarchy_level2, beta_mu_j)
    m1_beta_mu_j_keyBy_h2 = m1_beta_mu_j.keyBy(lambda (iter, hierarchy_level2, beta_mu_j): hierarchy_level2)
    # print "counts of m1_beta_mu_j ", m1_beta_mu_j.count() # number is 5 on both sides
     
    ## -- m1_beta_mu_j_draw : Draw beta_mu from mvnorm dist'n.  Get back J vectors of beta_mu, one for each J.
    ## Simply creates a join on  m1_beta_mu_j and  m1_Vbeta_inv_Sigmabeta_j_draw (the RDD keyby h2 equivalent is m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2 )
    ## extracts iter, hierarchy_level2 and beta_draw(beta_mu_j, Sigmabeta_j)
    joined_m1_beta_mu_j_with_m1_Vbeta_inv_Sigmabeta_j_draw_rdd = m1_beta_mu_j_keyBy_h2.cogroup(m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2)
    #  m1_beta_mu_j_draw <h2> => (iter, h2, beta_mu_j_draw)
    m1_beta_mu_j_draw = joined_m1_beta_mu_j_with_m1_Vbeta_inv_Sigmabeta_j_draw_rdd.map(gtr.get_beta_draw)
    m1_beta_mu_j_draw_keyBy_h2 = m1_beta_mu_j_draw.keyBy(lambda (iter, hierarchy_level2, beta_mu_j_draw): hierarchy_level2)
    # count of 5    
    # print "count m1_beta_mu_j_draw", m1_beta_mu_j_draw.count()
    # take 1 of <h2> => (iter, h2, beta_mu_j_draw)    
    # print "take 1 m1_beta_mu_j_draw", m1_beta_mu_j_draw.take(1)
    
    ## -- Compute Vbeta_i
    ## Uses a join of m1_d_array_agg_constants & m1_Vbeta_inv_Sigmabeta_j_draw
    ## m1_d_array_agg_constants is RDD of tuples h2,h1,xtx,xty
    ## m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2 is RDD of (key, Value)::(h2 => (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j))
    m1_d_array_agg_constants_key_by_h2 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2))
    # print "table 1 :",m1_d_array_agg_constants_key_by_h2.take(1)
    # print "table 1 count :",m1_d_array_agg_constants_key_by_h2.count()
    # print "table 2: ", m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2.take(1)
    # print "table 2 count 135: ",m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2.count()
    joined_m1_d_array_agg_constants_with_m1_Vbeta_inv_Sigmabeta_j_draw = m1_d_array_agg_constants_key_by_h2.cogroup(m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2)
    # count of 5    
    # print "count joined_m1_d_array_agg_constants_with_m1_Vbeta_inv_Sigmabeta_j_draw ", joined_m1_d_array_agg_constants_with_m1_Vbeta_inv_Sigmabeta_j_draw.count()
    # print "take 1 m1_beta_mu_j_draw", joined_m1_d_array_agg_constants_with_m1_Vbeta_inv_Sigmabeta_j_draw.take(1)
    # m1_Vbeta_i : iter, h2, h1, Vbeta_i
    m1_Vbeta_i_keyBy_h2_long = joined_m1_d_array_agg_constants_with_m1_Vbeta_inv_Sigmabeta_j_draw.map(lambda (x,y): (x, gtr.get_Vbeta_i(y)))
    # print "count m1_Vbeta_i", m1_Vbeta_i.count()
    # print "take 1 m1_Vbeta_i", m1_Vbeta_i.take(1)
      
    # -- Compute beta_i_mean
    m1_Vbeta_i = sc.parallelize(m1_Vbeta_i_keyBy_h2_long.values().reduce(add)).cache()
    m1_Vbeta_i_keyby_h2_h1 = m1_Vbeta_i.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i): (hierarchy_level2, hierarchy_level1))
    m1_d_array_agg_constants_key_by_h2_h1 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2, h1))
    # JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1 of tuples : hierarchy_level2, hierarchy_level1, Vbeta_i,xty
    JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1 = m1_Vbeta_i_keyby_h2_h1.cogroup(m1_d_array_agg_constants_key_by_h2_h1).map(lambda (x,y): (list(y[0])[0][1],list(y[0])[0][2], list(y[0])[0][3], list(y[1])[0][3]))
    JOINED_part_1_by_keyBy_h2 = JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1.keyBy(lambda (hierarchy_level2, hierarchy_level1, Vbeta_i, xty): hierarchy_level2)
    # print "table outer count ",  JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1.count()
    # print "table outer take 1",  JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1.take(1)     
    # Following is h2 ->(resIter1, resIter2) <=> where 
    # resIter1 is (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j)
    # resIter2 is (iter, hierarchy_level2, beta_mu_j_draw)
    JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw = m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2.cogroup(m1_beta_mu_j_draw_keyBy_h2).map(lambda (x,y): (x, list(y[0])[0][0], list(y[0])[0][3], list(y[1])[0][2]))
    JOINED_part_2_by_keyBy_h2 = JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.keyBy(lambda (hierarchy_level2, i, Vbeta_inv_j_draw, beta_mu_j_draw): hierarchy_level2)
    # print "take 1 ", JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.take(1) 
    # print "count 1 ", JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.count()
    m1_beta_i_mean_keyBy_h2_long = JOINED_part_1_by_keyBy_h2.cogroup(JOINED_part_2_by_keyBy_h2).map(lambda (x,y): (x, gtr.get_beta_i_mean(y)))
    # beta_i_mean = JOINED_part_1_by_keyBy_h2.cogroup(JOINED_part_2_by_keyBy_h2).map(lambda (x,y): (x, list(y[0]),list(y[1])))
    # print "beta_i_mean take ", m1_beta_i_mean.take(1) 
    # print "beta_i_mean count ", m1_beta_i_mean.count()
    
    #-- compute m1_beta_i_draw by  Draw beta_i from mvnorm dist'n
    # using m1_Vbeta_i_keyby_h2_h1 : h2, h1 => (i, hierarchy_level2, hierarchy_level1, Vbeta_i)
    # & parallelizing  beta_i_mean using h2, h1
    m1_beta_i_mean = sc.parallelize(m1_beta_i_mean_keyBy_h2_long.values().reduce(add))
    m1_beta_i_mean_keyBy_h2_h1 = m1_beta_i_mean.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, beta_i_mean): (hierarchy_level2, hierarchy_level1))
    # JOINED_m1_beta_i_mean_WITH_m1_Vbeta_i
    # m1_beta_i_draw : (iter, h2, h1, beta_i_draw)
    m1_beta_i_draw = m1_beta_i_mean_keyBy_h2_h1.cogroup(m1_Vbeta_i_keyby_h2_h1).map(lambda (x,y): (list(y[0])[0][0], x[0], x[1], gu.beta_draw(list(y[0])[0][3], list(y[1])[0][3])))
    print "m1_beta_i_draw take ", m1_beta_i_draw.take(1) 
    print "m1_beta_i_draw count ", m1_beta_i_draw.count() # 135
    
    ## -- Compute updated value of s2 to use in next section. 
    m1_beta_i_draw_group_by_h2_h1 = m1_beta_i_draw.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, beta_i_draw): (hierarchy_level2, hierarchy_level1))
    m1_d_array_agg_key_by_h2_h1 = m1_d_array_agg.keyBy(lambda (keys, x_matrix, y_array, hierarchy_level2, hierarchy_level1) : (keys[0], keys[1]))
    JOINED_m1_beta_i_draw_WITH_m1_d_array_agg = m1_d_array_agg_key_by_h2_h1.cogroup(m1_beta_i_draw_group_by_h2_h1)
    # print "JOINED_m1_beta_i_draw_WITH_m1_d_array_agg : 2 : ", JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.take(1)
    # hierarchy_level2, hierarchy_level1, x_array_var, y_var, iter, beta_i_draw
    JOINED_m1_beta_i_draw_WITH_m1_d_array_agg = JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.map(lambda (x, y): (x[0], x[1], list(y[0])[0][1], list(y[0])[0][2] ,list(y[1])[0][0], list(y[1])[0][3], m1_d_count_grpby_level2_b.value[x[0]]))
    # print "JOINED_m1_beta_i_draw_WITH_m1_d_array_agg : 3 :", JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.take(1)
    # JOINED_m1_beta_i_draw_WITH_m1_d_array_agg = JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.map(lambda (x, y): (x[0], x[1], list(list(y[0])[0])[1], list(list(y[0])[0])[2], list(y[1])[0][0], list(y[1])[0][3], m1_d_count_grpby_level2_b.value[x[0]]))
    foo = JOINED_m1_beta_i_draw_WITH_m1_d_array_agg.keyBy(lambda (hierarchy_level2, hierarchy_level1, x_array_var, y_var, iteri, beta_i_draw, m1_d_count_grpby_level2_b): (hierarchy_level2, hierarchy_level1, iteri))
    # print "foo : 4 : ", foo.take(1)
    # print "foo : 4 : ", foo.count()
    # foo2 is group by hierarchy_level2, hierarchy_level1, iteri and has structure as ey => ( hierarchy_level2, hierarchy_level1, iteri, ssr, m1_d_count_grpby_level2_b )
    foo2 = foo.map(lambda (x, y): gtr.get_sum_beta_i_draw_x2(y)).keyBy(lambda (hierarchy_level2, hierarchy_level1, iteri, ssr, m1_d_count_grpby_level2_b): (hierarchy_level2, iteri, m1_d_count_grpby_level2_b))
    # print "foo2 : 5 : ", foo2.take(1)
    # print "foo2 : 5 : ", foo2.count()
    # foo3 = foo2.groupByKey().map(lambda (x, y): get_s2(list(y)))
    # iteri, hierarchy_level2, m1_d_count_grpby_level2_b, s2
    m1_s2 = foo2.groupByKey().map(lambda (x, y): gtr.get_s2(list(y)))
    print "m1_s2 : 5 : ", m1_s2.take(1)
    print "m1_s2 : 5 : ", m1_s2.count() 
    
    ### -- Draw h from gamma distn.  Note that h=1/(s^2)
    ## from iteri, hierarchy_level2, m1_d_count_grpby_level2_b, s2
    ## m1_h_draw = iteri, h2, h_draw
    m1_h_draw = m1_s2.map(gtr.get_h_draw)
    print "m1_h_draw : 5 : ", m1_h_draw.take(1)
    print "m1_h_draw : 5 : ", m1_h_draw.count() 
    
    print gibbs_init_text()    
    
    return (m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2_b ,m1_h_draw , m1_ols_beta_i ,m1_ols_beta_j ,m1_s2 ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_j_mu)
    