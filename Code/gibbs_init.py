import numpy as np
from pyspark import SparkContext
import sys
import gibbs_udfs as gu
import nearPD as npd

#'p_var' = Number of explanatory variables in the model, including the intercept term.
p_var = 14
accum = 0
df1_var = 15
# both Arrays below has one less element. Looks like there was a one in row or
# column somewhere in the computations that was missed out. I was to proceed without any
# corrections at this point of time and revisit this problem again.
coef_precision_prior_array_var = [1,1,1,1,1,1,1,1,1,1,1,1,1]
coef_means_prior_array_var = [0,0,0,0,0,0,0,0,0,0,0,0,0]

def create_x_matrix_y_array(recObj):
    """
       Take an iterable of records, where the key corresponds to a certain age group
       Create a numpy matrix and return the shape of the matrix
    """
    import numpy

    #recObj is of the form of [key, <Iterable of all values tuples>]
    keys = recObj[0]
    recIter = recObj[1]

    mat = numpy.matrix([r for r in recIter])
    x_matrix = mat[:,5:18].astype(float)
    y_array = mat[:,4].astype(float)
    hierarchy_level2 =  mat[:,2]
    hierarchy_level1 = mat[:,1]
    return (keys, x_matrix, y_array, hierarchy_level2[1,0], hierarchy_level1[1,0])


def create_xtx_matrix_xty(obj):
    import numpy
    #recObj is of the form of [key, <Iterable of all values tuples>]
    keys = obj[0]
    x_matrix = obj[1]
    x_matrix_t = numpy.transpose(x_matrix)
    xt_x = x_matrix_t * x_matrix
    y_matrix = obj[2]
    xt_y = x_matrix_t * y_matrix
    # h2, h1, xtx, xty
    return (keys[0], keys[1], xt_x, xt_y)

def get_d_childcount(obj):
    keyBy_h2_to_h1 = obj.map(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).groupByKey()
    # returns DS with key hierarchy_level2 and value <hierarchy_level2, n1>
    return keyBy_h2_to_h1.map(lambda (x,iter): (x, sum(1 for _ in set(iter))))

def get_d_count_grpby_level2(obj):
    keyBy_h2_week = obj.map(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, week))
    return keyBy_h2_week
    # return keyBy_h2.map(lambda (x,iter): (x, sum(1 for _ in set(iter))))

def get_ols_initialvals_beta_i(obj):
    from sklearn import linear_model
    regr = linear_model.LinearRegression()
    # fit x_array, y_array
    regr.fit(obj[1], obj[2])
    print('Coefficients: \n', regr.coef_)
    return (obj[3], obj[4], regr.coef_)


def get_ols_initialvals_beta_j(obj):
    from sklearn import linear_model
    regr = linear_model.LinearRegression()
    # fit x_array, y_array
    regr.fit(obj[1], obj[2])
    print('Coefficients: \n', regr.coef_)
    #hierarchy_level2 = a matrix obj[3] of same values in hierarchy_level2
    return (obj[3], regr.coef_)


def get_random_initialvals_beta_i(obj):
    coeff = gu.initial_vals_random(p_var)
    #hierarchy_level2 = obj[3]
    #hierarchy_level1 = obj[4]
    return (obj[3], obj[4], coeff)


def get_random_initialvals_beta_j(obj):
    coeff = gu.initial_vals_random(p_var)
    #hierarchy_level2 = obj[3]
    #hierarchy_level1 = obj[4]
    return (obj[3], coeff)


def create_join_by_h2_only(t1,t2):
    # t1 is a list of ((u'"1"', u'"B8"'), (u'"1"', u'"B8"', array([[  7.16677290e-01,   4.15236265e-01,   7.02316511e-02,
    # t2 is a list of (u'"5"', (u'"5"', array([[ 0.86596322,  0.29811589,  0.29083844
    joined = []
    for rec1 in t1:
        keys = rec1[0]
        hierarchy_level2 = keys[0]
        hierarchy_level1 = keys[1]
        values_array_i = rec1[1][2]
        for rec2 in t2:
            hierarchy_level2_rec2 = rec2[0]
            values_array_j = rec2[1][1]
            if(hierarchy_level2 == hierarchy_level2_rec2):
                tup = (hierarchy_level2, hierarchy_level1, values_array_i, values_array_j)
                joined.append(tup)
    return joined

# funciton used to compute an appended list of coeff_i and coeff_j for the same
# hierarchical level. Used in get_Vbeta_j_mu
def get_Vbeta_i_mu_coeff_i_coeff_j(result_Iterable_list):
    Vbeta_i_mu_ar = []
    for r in result_Iterable_list:
        values_array_i = r[2]
        values_array_j = r[3]
        Vbeta_i_mu_ar.append(gu.Vbeta_i_mu(values_array_i, values_array_j))
    return Vbeta_i_mu_ar


def get_Vbeta_j_mu(obj):
    global accum;
    accum += 1
    keys = obj[0] # hierarchy_level2
    # now Obj1 is an ResultIterable object pointing to a collection of arrays
    # where each array has a structure like <h2,h1,coef_i,coef_j>
    result_Iterable_list = list(obj[1])
    Vbeta_i_mu_ar = get_Vbeta_i_mu_coeff_i_coeff_j(result_Iterable_list)
    # one can also obtain Vbeta_i_mu_sum as  map(lambda (x,y): (x, sum(fun(list(y)))), joined_i_j_rdd.take(1))
    # corresponding to each one of the h2 level
    Vbeta_i_mu_sum = sum(Vbeta_i_mu_ar)
    Vbeta_j_mu = gu.matrix_add_diag_plr(Vbeta_i_mu_sum, p_var)
    # iter, hierarchy_level2, Vbeta_j_mu
    return accum, keys, Vbeta_j_mu


def get_m1_Vbeta_j_mu_pinv(obj):
    import numpy as np
    import nearPD as npd
    global df1_var
    seq = obj[0]
    hierarchy_level2 = obj[1]
    Vbeta_j_mu = obj[2]
    # Vbeta_inv_draw(nu, phi) where nu is df1_var & for phi matrix we have
    a = gu.matrix_scalarmult_plr(Vbeta_j_mu, df1_var)
    phi = np.linalg.pinv(a)
    # is phi is not positive definiate matrix than compute a nearPD for it
    if np.all(np.linalg.eigvals(phi) > 0) != True:
        phi = npd.nearPD(phi)
    Vbeta_inv_j_draw = gu.Vbeta_inv_draw(df1_var, phi)
    return (seq, hierarchy_level2, Vbeta_inv_j_draw)


def np_pinv(Vbeta_inv_j_draw, n1, coef_precision_prior_array):
    import numpy as np
    temp = gu.matrix_scalarmult_plr(Vbeta_inv_j_draw, n1)
    temp_add = gu.matrix_scalarmult_plr(temp, gu.matrix_diag_plr(coef_precision_prior_array))
    return np.linalg.pinv(temp_add)
    
def get_m1_Vbeta_inv_Sigmabeta_j_draw(lst):
    global coef_precision_prior_array_var
    #y[0][0], y[0][1], y[1][1] , y[0][2], np_pinv(y[0][2], y[1][1], coef_precision_prior_array_var)
    #or seq from m1_Vbeta_j_mu_pinv
    iter = 0 
    #from in m1_Vbeta_j_mu_pinv
    h2 = "" 
    # from m1_d_childcount_groupBy_h2,
    n1 = 1
    #  Vbeta_inv_j_draw =  from m1_Vbeta_j_mu_pinv, np_pin()
    Vbeta_inv_j_draw = None
    for r in lst[0]:
	for j in r:
	    iter = j[0]
	    h2 = j[1]
	    Vbeta_inv_j_draw = j[2]
    for r in lst[1]:
	for j in r:
	    print "n1: ", j[1]
	    n1 = j[1]
    
    return (iter, h2, n1, Vbeta_inv_j_draw, np_pinv(Vbeta_inv_j_draw, n1, coef_precision_prior_array_var))


def get_substructure_beta_mu_j(obj):
    global coef_precision_prior_array_var, coef_means_prior_array_var
    # (k, (W1, W2)) 
    #  k = hierarchy_level2
    # where W1 is a ResultIterable having iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j
    # and W2 is another ResultIterable having sum_coef_j
    for r in obj[1][0]:
        for j in r:
            iteri = j[0]
            hierarchy_level2 = j[1]
            n1 = j[2]
            Vbeta_inv_j_draw = j[3]
            Sigmabeta_j = j[4]
    for r in obj[1][1]:
        sum_coef_j = r[0]
    beta_mu_j = gu.beta_mu_prior(Sigmabeta_j, Vbeta_inv_j_draw, sum_coef_j, coef_means_prior_array_var, coef_precision_prior_array_var)
    return (iteri, hierarchy_level2, beta_mu_j)

def add_coeff_j(hierarchy_level2, iterable_object):
    # where each iterable is like (hierarchy_level2, array[[]] of dim 1X13)
    array_list = []
    for r in iterable_object:
        array_list.append(r[1])
    sum_coef_j = sum(array_list) 
    return (hierarchy_level2, sum_coef_j)
    

def get_beta_draw(obj):
    key = obj[0]
    # key is hierarchy_level2 and 
    # cogrouped_iterable_object is <W1,W2>
    # where W1 is a ResultIterable having iter, hierarchy_level2, beta_mu_j
    # and W2 is another ResultIterable having iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j
    for j in obj[1][0]:
        iteri = j[0]
        beta_mu_j = j[2]
    for r in obj[1][1]:
        for j in r:
            Sigmabeta_j = j[4]
    return (iteri, key, gu.beta_draw(beta_mu_j, Sigmabeta_j))

def gibbs_init(model_name, source_RDD, hierarchy_level1, hierarchy_level2, p, df1, y_var, x_var_array, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals):
    text_output = 'Done: Gibbs Sampler for model model_name is initialized.  Proceed to run updates of the sampler by using the gibbs() function.  All objects associated with this model are named with a model_name prefix.'
    return text_output


def gibbs_init_test(sc, d, keyBy_groupby_h2_h1, initial_vals, p):
    #if __name__ == '__main__':
    global p_var
    global coef_precision_prior_array_var
    # sc = SparkContext(appName="gibbs_init")
    # of the form keys, x_array, y_array
    m1_d_array_agg = keyBy_groupby_h2_h1.map(create_x_matrix_y_array)
    #  we need to make use of X'X and X'y
    #  m1_d_array_agg_constants : list of tuples of (h2, h1, xtx, xty)
    m1_d_array_agg_constants = m1_d_array_agg.map(create_xtx_matrix_xty)
    #print m1_d_array_agg_constants.take(1)
    # Compute the childcount at each hierarchy level
    # computing the number of hierarchy_level2 nodes for each of the hierarchy_level1 node
    # think of h2 as department and h1 as the stores
    # the following computes the number of stores in each department
    m1_d_childcount = get_d_childcount(d)
    #print "d_child_counts are : ", m1_d_childcount.count()
    # since the number of weeks of data for each deparment_name-tiers is different.
    # we wll precompute this quantity for each department_name-tier
    m1_d_count_grpby_level2 = get_d_count_grpby_level2(d)
    #print "Available data for each department_name-tiers", m1_d_count_grpby_level2.countByKey()
    # structure to compute maps by h2 as key only at m1_d_array_agg levels
    keyBy_h2 = d.keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2)).groupByKey().map(create_x_matrix_y_array)
    if(initial_vals == "ols"):
        # Compute OLS estimates for reference
        m1_ols_beta_i = m1_d_array_agg.map(get_ols_initialvals_beta_i).keyBy(lambda (h2,h1,coff): (h2, h1))
        #print "Coefficients for LL after keyby H2", m1_ols_beta_i.count()
        # similarly we compute the m1_ols_beta_j which uses the RDD mapped upon only hierarchy_level2
        m1_ols_beta_j = keyBy_h2.map(get_ols_initialvals_beta_j).keyBy(lambda (h2,coff): (h2))
        #print "Coefficients for LL after keyby H2", m1_ols_beta_j.count()
    # in case the initial_vals are defined as "random" we compute the exact same
    # data structures using deviates from Uniform distribution
    if(initial_vals == "random"):
        print "Draw random array samples of p elements from the uniform(-1,1) dist'n"
        p_var = p
        m1_ols_beta_i = m1_d_array_agg.map(get_random_initialvals_beta_i).keyBy(lambda (h2,h1,coff): (h2, h1))
        m1_ols_beta_j = keyBy_h2.map(get_random_initialvals_beta_j).keyBy(lambda (h2,coff): (h2))
    #-- Using the above initial values of the coefficients and drawn values of priors,
    #   compute initial value of coefficient var-cov matrix (Vbeta_i_mu)
    #   FOR EACH group i, with group j coefficients as priors, and
    #   then sum then to get back J matrices
    # computing _Vbeta_j_mu
      
    joined_i_j = create_join_by_h2_only(m1_ols_beta_i.collect(), m1_ols_beta_j.collect())
    # keyBy and groupBy will reduce the rows from 135 to 5 since there are only 5 hierarchy_level2's
    joined_i_j_rdd = sc.parallelize(joined_i_j).keyBy(lambda (hierarchy_level2, hierarchy_level1, values_array_i, values_array_j): (hierarchy_level2)).groupByKey()
    # joined_i_j_rdd.take(1) :  (u'"5"', <pyspark.resultiterable.ResultIterable object at 0x117be50>) similarly 5 others
    # CHECKPOINT for get_Vbeta_j_mu
    ## checked get_Vbeta_j_mu & appears correct one,
    ## Data Structure m1_Vbeta_j_mu is symmetric along diagonal and have same dimensions as the one in SQL.
    m1_Vbeta_j_mu = joined_i_j_rdd.map(get_Vbeta_j_mu)
     
    #print " m1_Vbeta_j_mu count ", m1_Vbeta_j_mu.count() # the actual values are 500 I am getting 135 values
    #print " m1_Vbeta_j_mu take 1", m1_Vbeta_j_mu.take(1)
    ###-- Draw Vbeta_inv and compute resulting sigmabeta using the above functions for each j
    """
    Errorsome on "matrix is not positive definite." raised by
    File "gibbs_init.py", line 133, in get_m1_Vbeta_j_mu_pinv
    Vbeta_inv_j_draw = gu.Vbeta_inv_draw(df1_var, phi)
    File "gibbs_udfs.py", line 84, in Vbeta_inv_draw
    return wishartrand(nu, phi)
    CHECKPOINT for get_Vbeta_j_mu ## changed one of the phi matrixs into
    definite positive using nearPD in python
    """
    m1_Vbeta_j_mu_pinv = m1_Vbeta_j_mu.map(get_m1_Vbeta_j_mu_pinv).keyBy(lambda (seq, hierarchy_level2, Vbeta_inv_j_draw) : (hierarchy_level2)).groupByKey() 
    m1_d_childcount_groupBy_h2 = m1_d_childcount.keyBy(lambda (hierarchy_level2, n1) : hierarchy_level2).groupByKey()
    #  here vals are iter, h2,
    #  y[0][0] = iter or seq from m1_Vbeta_j_mu_pinv
    #  y[0][1] = h2 from in m1_Vbeta_j_mu_pinv
    #  y[1][1] = n1 from m1_d_childcount_groupBy_h2,
    #  y[0][2] = Vbeta_inv_j_draw from m1_Vbeta_j_mu_pinv, np_pin()
    #  error 'ResultIterable' object does not support indexing
    #  map(lambda (x,y): (x, sum(fun(list(y)))), joined_i_j_rdd.take(1))
    joined_Vbeta_i_j = sorted(m1_Vbeta_j_mu_pinv.cogroup(m1_d_childcount_groupBy_h2).collect())
    #print " cogroup counts: ", len(joined_Vbeta_i_j)
    #print " Vbeta_i_j cogroup take 1", joined_Vbeta_i_j[1]
    #print "map ", map(lambda (x,y): (x, (y for y in list(y[0]))), joined_Vbeta_i_j)
    m1_Vbeta_inv_Sigmabeta_j_draw = map(lambda (x,y): get_m1_Vbeta_inv_Sigmabeta_j_draw(list(y)), joined_Vbeta_i_j) 
    #print " m1_Vbeta_inv_Sigmabeta_j_draw Take 1: ", m1_Vbeta_inv_Sigmabeta_j_draw[1]
    #print " m1_Vbeta_inv_Sigmabeta_j_draw Count: ", len(m1_Vbeta_inv_Sigmabeta_j_draw)
    m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2 = sc.parallelize(m1_Vbeta_inv_Sigmabeta_j_draw).keyBy(lambda (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j): (hierarchy_level2)).groupByKey() 
    
    ##-- Compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  
    ##-- Get back one coefficient vector for each j (i.e. J  coefficient vectors are returned).
    ## computing _beta_mu_j
    ## for computing _beta_mu_j we first will modify m1_ols_beta_i or _initialvals_beta_i to get sum_coef_j 
    ## and then  we will join it with m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2
    m1_ols_beta_i_sum_coef_j = m1_ols_beta_i.map(lambda (x,y): (x[0], y[2])).keyBy(lambda (h2, coeff): h2).groupByKey().map(lambda (key, value) : add_coeff_j(key,value))
    #print "Diagnostics m1_ols_beta_i_sum_coef_j ", m1_ols_beta_i_sum_coef_j.collect()    
    joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j = m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2.cogroup(m1_ols_beta_i_sum_coef_j)
    #print "joined_m1_Vbeta_inv ", joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j.take(1)  
    # m1_beta_mu_j is  RDD keyed structure as hierarchy_level2=> (iter, hierarchy_level2, beta_mu_j)
    # beta_mu_j is mean with dim 13 X 1
    m1_beta_mu_j = joined_m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2_m1_ols_beta_i_sum_coef_j.map(get_substructure_beta_mu_j).keyBy(lambda (iter, hierarchy_level2, beta_mu_j): hierarchy_level2)
    #print "counts of m1_beta_mu_j ", m1_beta_mu_j.count() # number is 5 on both sides
     
    ## -- Draw beta_mu from mvnorm dist'n.  Get back J vectors of beta_mu, one for each J.
    ## Simply creates a join on  m1_beta_mu_j and  m1_Vbeta_inv_Sigmabeta_j_draw (the RDD keyby h2 equivalent is m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2 )
    ## extracts iter, hierarchy_level2 and beta_draw(beta_mu_j, Sigmabeta_j)
    joined_m1_beta_mu_j_with_m1_Vbeta_inv_Sigmabeta_j_draw_rdd = m1_beta_mu_j.cogroup(m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2)
    m1_beta_mu_j_draw = joined_m1_beta_mu_j_with_m1_Vbeta_inv_Sigmabeta_j_draw_rdd.map(get_beta_draw).keyBy(lambda (iter, hierarchy_level2, beta_mu_j_draw): hierarchy_level2)
    # count of 5    
    print "count m1_beta_mu_j_draw", m1_beta_mu_j_draw.count()
    # take 1 of <h2> => (iter, h2, beta_mu_j_draw)    
    print "take 1 m1_beta_mu_j_draw", m1_beta_mu_j_draw.take(1)
    
    """
    4 more DS after that """
    ## -- Compute Vbeta_i
    ## Uses a join of m1_d_array_agg_constants & m1_Vbeta_inv_Sigmabeta_j_draw
    ## m1_d_array_agg_constants is RDD of tuples h2,h1,xtx,xty
    ## m1_Vbeta_inv_Sigmabeta_j_draw is RDD of (key, Value):(h2 => (iter, hierarchy_level2, beta_mu_j_draw))
        
    
    # exp with cogroup
    #join_coefi_coefj = map(lambda (x, y): (x, (list(y[0]), list(y[1]))),
    #   sorted(m1_ols_beta_i.cogroup(m1_ols_beta_j).collect()))
    #join_coefi_coefj = m1_ols_beta_i.cogroup(m1_ols_beta_j)
     
    #len(join_coefi_coefj)
    #m1_Vbeta_j_mu =













