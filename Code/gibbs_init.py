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
sample_size_deflator = 1

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
    #return keyBy_h2.map(lambda (x,iter): (x, sum(1 for _ in set(iter))))

def get_ols_initialvals_beta_i(obj):
    from sklearn import linear_model
    regr = linear_model.LinearRegression()
    # fit x_array, y_array
    regr.fit(obj[1], obj[2])
    return (obj[3], obj[4], regr.coef_)


def get_ols_initialvals_beta_j(obj):
    from sklearn import linear_model
    regr = linear_model.LinearRegression()
    # fit x_array, y_array
    regr.fit(obj[1], obj[2])
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


def get_Vbeta_j_mu(y):
    # now y is an ResultIterable object pointing to a collection of arrays
    # where each array has a structure like <h2,h1,coef_i,coef_j>
    result_Iterable_list = list(y)
    Vbeta_i_mu_ar = get_Vbeta_i_mu_coeff_i_coeff_j(result_Iterable_list)
    # one can also obtain Vbeta_i_mu_sum as  map(lambda (x,y): (x, sum(fun(list(y)))), joined_i_j_rdd.take(1))
    # corresponding to each one of the h2 level
    Vbeta_i_mu_sum = sum(Vbeta_i_mu_ar)
    Vbeta_j_mu = gu.matrix_add_diag_plr(Vbeta_i_mu_sum, p_var)
    # iter, hierarchy_level2, Vbeta_j_mu
    return Vbeta_j_mu


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
    return (seq, hierarchy_level2, Vbeta_inv_j_draw.real)


def pinv_Vbeta_inv_Sigmabeta_j_draw(Vbeta_inv_j_draw, n1, coef_precision_prior_array):
    import numpy as np
    temp = gu.matrix_scalarmult_plr(Vbeta_inv_j_draw, n1)
    temp_add = gu.matrix_scalarmult_plr(temp, gu.matrix_diag_plr(coef_precision_prior_array))
    return np.linalg.pinv(temp_add)
    
def get_m1_Vbeta_inv_Sigmabeta_j_draw(lst):
    global coef_precision_prior_array_var
    #y[0][0], y[0][1], y[1][1] , y[0][2], pinv_Vbeta_inv_Sigmabeta_j_draw(y[0][2], y[1][1], coef_precision_prior_array_var)
    #or seq from m1_Vbeta_j_mu_pinv
    iter = 0 
    #from in m1_Vbeta_j_mu_pinv
    h2 = "" 
    # from m1_d_childcount_groupBy_h2,
    n1 = 1
    #  Vbeta_inv_j_draw =  from m1_Vbeta_j_mu_pinv, pinv_Vbeta_inv_Sigmabeta_j_draw()
    Vbeta_inv_j_draw = None
    for r in lst[0]:
        for j in r:
            iter = j[0]
            h2 = j[1]
            Vbeta_inv_j_draw = j[2]
    for r in lst[1]:
        for j in r:
            n1 = j[1]
    
    return (iter, h2, n1, Vbeta_inv_j_draw, pinv_Vbeta_inv_Sigmabeta_j_draw(Vbeta_inv_j_draw, n1, coef_precision_prior_array_var))


def get_substructure_beta_mu_j(obj):
    global coef_precision_prior_array_var, coef_means_prior_array_var
    # (k, (W1, W2)) 
    #  k = hierarchy_level2
    # where W1 is a ResultIterable having iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j
    # and W2 is another ResultIterable having sum_coef_j
    for r in obj[1][0]:
        #for j in r:
        iteri = r[0]
        hierarchy_level2 = r[1]
        n1 = r[2]
        Vbeta_inv_j_draw = r[3]
        Sigmabeta_j = r[4]
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
        Sigmabeta_j = r[4]
    return (iteri, key, gu.beta_draw(beta_mu_j, Sigmabeta_j))
    
def pinv_Vbeta_i(xtx, Vbeta_inv_j_draw, s):
    return gu.matrix_add_plr(gu.matrix_scalarmult_plr(xtx, s), Vbeta_inv_j_draw)    
    #return (type(xtx), type(Vbeta_inv_j_draw))
    
def get_Vbeta_i(obj):
    # key is hierarchy_level2 and 
    # cogrouped_iterable_object is <W1,W2>
    # where W2 is a ResultIterable having hierarchy_level2 => (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j))
    for r in obj[1]:
        h2 = r[1]
        Vbeta_inv_j_draw = r[3]
    rows = []
    #count = 1
    # obj[0] where W1 is a ResultIterable having obj[1][0]=hierarchy_level2, obj[1][1]=hierarchy_level1, xtx, xty
    for r in obj[0]:
        hierarchy_level2 = r[0]
        if hierarchy_level2 != h2:
            raise NameError('Index not correct')
        hierarchy_level1 =r[1]
        xtx = r[2]
        Vbeta_i = pinv_Vbeta_i(xtx, Vbeta_inv_j_draw, 1)
        row = (1, hierarchy_level2, hierarchy_level1, Vbeta_i)
        #count += 1
        rows.append(row)
    # iteration is 1 so returning only 1
    # row = (1, hierarchy_level2, hierarchy_level1, Vbeta_i)
    return (rows)

def get_Vbeta_i_next(obj, s):
    # key is hierarchy_level2 and 
    # cogrouped_iterable_object is <W1,W2>
    # where W2 is a ResultIterable having hierarchy_level2 => (iter, hierarchy_level2, Vbeta_inv_j_draw, h_draw))
    obj1 = list(obj[1])[0]
    h2 = obj1[1]
    Vbeta_inv_j_draw = obj1[2]
    h_draw = obj1[3]
    
    rows = []
    # obj[0] where W1 is a ResultIterable having obj[1][0]=hierarchy_level2, obj[1][1]=hierarchy_level1, xtx, xty
    for r in obj[0]:
        hierarchy_level2 = r[0]
        if hierarchy_level2 != h2:
            raise NameError('Index not correct')
        hierarchy_level1 =r[1]
        xtx = r[2]
        Vbeta_i = pinv_Vbeta_i(xtx, Vbeta_inv_j_draw, h_draw)
        row = (s, hierarchy_level2, hierarchy_level1, Vbeta_i) 
        rows.append(row)
    # row = (s, hierarchy_level2, hierarchy_level1, Vbeta_i)
    return (rows)
    
def get_beta_i_mean(y, s):
    # y[0] is iterable results of a list of tuples <h2, h1, Vbeta_i, xty>
    # y[1] is iterable results of a tuple with values <h2, iter, Vbeta_inv_j_draw, beta_mu_j_draw>
    for r in y[1]:
      hierarchy_level2 = r[0]
      i = r[1]
      Vbeta_inv_j_draw = r[2]
      beta_mu_j_draw = r[3]
    result_list = []
    for j in y[0]:
        hierarchy_level1 = j[1]
        Vbeta_i = j[2]
        xty = j[3]
        beta_i_mean = gu.beta_i_mean(Vbeta_i, 1, xty, Vbeta_inv_j_draw, beta_mu_j_draw)
        row = (i, hierarchy_level2, hierarchy_level1, beta_i_mean)
        result_list.append(row)
    return result_list
    
def get_beta_i_mean_next(y, s):
    # y[0] is iterable results of a list of tuples <h2, h1, Vbeta_i, xty>
    # y[1] is iterable results of a tuple with values <h2, h_draw, Vbeta_inv_j_draw, beta_mu_j_draw>
    r = list(y[1])[0]
    hierarchy_level2 = r[0]
    h_draw = r[1]
    Vbeta_inv_j_draw = r[2]
    beta_mu_j_draw = r[3]
    
    result_list = []
    for j in y[0]:
        hierarchy_level1 = j[1]
        Vbeta_i = j[2]
        xty = j[3]
        beta_i_mean = gu.beta_i_mean(Vbeta_i, h_draw, xty, Vbeta_inv_j_draw, beta_mu_j_draw)
        row = (s, hierarchy_level2, hierarchy_level1, beta_i_mean)
        result_list.append(row)
    return result_list
    
# depricated    
def getX_var_array_y_array(y_0_list):
    import numpy as np
    #index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13
    #
    y_var_list = []
    x_var_array_list = []
    for row in y_0_list:
        #y_var_list.append([float(i) for i in row[4]])
        #x_var_array_list.append([float(i) for i in row[5:18]])
        y_var_list.append(row[4])
        x_var_array_list.append(row[5:18])
    y_var = np.array(y_var_list)
    x_var_array = np.array(x_var_array_list)
    return (x_var_array, y_var)
    
# depricated
def get_sum_beta_i_draw_x(y):
    import numpy as np
    # where y is a tuple with value hierarchy_level2, hierarchy_level1, x_array_var_y_var, iteri, beta_i_draw, m1_d_count_grpby_level2_b
    #ssr = sum((y-madlib.array_dot(beta_i_draw, x))^2)
    beta_i_draw = y[4]
    x_array_var = y[2][0]
    y_var = y[2][1]
    ssr = np.subtract(y_var, np.dot(beta_i_draw, x_array_var))
    # hierarchy_level2, hierarchy_level1, iteri, ssr, m1_d_count_grpby_level2_b
    return (y[0], y[1], y[3], ssr, y[5])    

def get_sum_beta_i_draw_x2(y): 
    # hierarchy_level2, hierarchy_level1, x_array_var, y_var, iteri, beta_i_draw, m1_d_count_grpby_level2_b
    x_array_var = y[2]
    y_var = y[3]
    beta_i_draw = y[5]
    lsts_squares_of_sub_of_dot = []
    for x_var in x_array_var:
        ssr_sm = np.power(np.subtract(y_var, np.dot(beta_i_draw, x_var.getA1())), 2)
        lsts_squares_of_sub_of_dot.append(ssr_sm)
    ssr = sum(lsts_squares_of_sub_of_dot).item(0)
    return (y[0], y[1], y[4], ssr, y[6]) 
    
def get_s2(y):
    # hierarchy_level2, hierarchy_level1, iteri, ssr, m1_d_count_grpby_level2_b
    # where y is a set of result iterables objects : 
    global sample_size_deflator
    list_ssrs = []
    prelims = y[0]
    hierarchy_level2 = prelims[0]
    iteri = prelims[2]
    m1_d_count_grpby_level2_b = prelims[4]
    for rec in y:
        list_ssrs.append(rec[3])
    s2 = sum(list_ssrs)/(m1_d_count_grpby_level2_b / sample_size_deflator)
    return (iteri, hierarchy_level2, m1_d_count_grpby_level2_b, s2)
    
def get_h_draw(x):
    global sample_size_deflator
    ## iteri, hierarchy_level2, m1_d_count_grpby_level2_b, s2
    iteri = x[0]
    hierarchy_level2 = x[1]
    m1_d_count_grpby_level2_b = x[2]
    s2 = x[3]
    h_draw = gu.h_draw(1.0/(s2), m1_d_count_grpby_level2_b/sample_size_deflator)[0]
    return (iteri, hierarchy_level2, h_draw)

def gibbs_init(model_name, source_RDD, hierarchy_level1, hierarchy_level2, p, df1, y_var, x_var_array, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals):
    text_output = 'Done: Gibbs Sampler for model model_name is initialized.  Proceed to run updates of the sampler by using the gibbs() function.  All objects associated with this model are named with a model_name prefix.'
    return text_output

def add(x,y):
    return (x+y)

def gibbs_init_test(sc, d, keyBy_groupby_h2_h1, initial_vals, p):
    #if __name__ == '__main__':
    global p_var
    global coef_precision_prior_array_var
    
    # sc = SparkContext(appName="gibbs_init")
    # of the form keys, x_matrix, y_array, hierarchy_level2[1,0], hierarchy_level1[1,0]
    m1_d_array_agg = keyBy_groupby_h2_h1.map(create_x_matrix_y_array)
    #  we need to make use of X'X and X'y
    #  m1_d_array_agg_constants : list of tuples of (h2, h1, xtx, xty)
    m1_d_array_agg_constants = m1_d_array_agg.map(create_xtx_matrix_xty)
    # m1_d_array_agg_constants.saveAsTextFile("hdfs://sandbox:9000/m1_d_array_agg_constants")
    # print "m1_d_array_agg_constants take ",m1_d_array_agg_constants.take(1)
    # print "m1_d_array_agg_constants count",m1_d_array_agg_constants.count()
    # Compute the childcount at each hierarchy level
    # computing the number of hierarchy_level2 nodes for each of the hierarchy_level1 node
    # think of h2 as department and h1 as the stores
    # the following computes the number of stores in each department
    m1_d_childcount = get_d_childcount(d)
    #print "d_child_counts are : ", m1_d_childcount.count()
    # since the number of weeks of data for each deparment_name-tiers is different.
    # we wll precompute this quantity for each department_name-tier
    
    m1_d_count_grpby_level2 = get_d_count_grpby_level2(d)
    print "takes m1_d_count_grpby_level2 : ", m1_d_count_grpby_level2.take(1)
    print "count m1_d_count_grpby_level2 : ", m1_d_count_grpby_level2.count()
    #m1_d_count_grpby_level2 = sc.parallelize(m1_d_count_grpby_level2).keyBy(lambda (hierarchy_level2, countsp): hierarchy_level2)
    print "Available data for each department_name-tiers", m1_d_count_grpby_level2.countByKey()
    # m1_d_count_grpby_level2.countByKey() becomes defaultdict of type int As
    # defaultdict(<type 'int'>, {u'"5"': 1569, u'"1"': 3143, u'"2"': 3150, u'"3"': 3150, u'"4"': 3150})
    m1_d_count_grpby_level2 = m1_d_count_grpby_level2.countByKey()
    m1_d_count_grpby_level2_b = sc.broadcast(m1_d_count_grpby_level2)
    #print "bc value", m1_d_count_grpby_level2_b.value
    #print "bc ", m1_d_count_grpby_level2_b
    
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
        #print "Draw random array samples of p elements from the uniform(-1,1) dist'n"
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
    m1_Vbeta_j_mu = joined_i_j_rdd.map(lambda (x, y): (1, x, get_Vbeta_j_mu(y)))
     
    #print " m1_Vbeta_j_mu count ", m1_Vbeta_j_mu.count() # the actual values are 500 I am getting 135 values
    #print " m1_Vbeta_j_mu take 1", m1_Vbeta_j_mu.take(1)
    ###-- Draw Vbeta_inv and compute resulting sigmabeta using the above functions for each j
    
    m1_Vbeta_j_mu_pinv = m1_Vbeta_j_mu.map(get_m1_Vbeta_j_mu_pinv).keyBy(lambda (seq, hierarchy_level2, Vbeta_inv_j_draw) : (hierarchy_level2)).groupByKey() 
    m1_d_childcount_groupBy_h2 = m1_d_childcount.keyBy(lambda (hierarchy_level2, n1) : hierarchy_level2).groupByKey()
    #  here vals are iter, h2,
    #  y[0][0] = iter or seq from m1_Vbeta_j_mu_pinv
    #  y[0][1] = h2 from in m1_Vbeta_j_mu_pinv
    #  y[1][1] = n1 from m1_d_childcount_groupBy_h2,
    #  y[0][2] = Vbeta_inv_j_draw from m1_Vbeta_j_mu_pinv, pinv_Vbeta_inv_Sigmabeta_j_draw()
    #  error 'ResultIterable' object does not support indexing
    #  map(lambda (x,y): (x, sum(fun(list(y)))), joined_i_j_rdd.take(1))
    joined_Vbeta_i_j = sorted(m1_Vbeta_j_mu_pinv.cogroup(m1_d_childcount_groupBy_h2).collect())
    #print " cogroup counts: ", len(joined_Vbeta_i_j)
    #print " Vbeta_i_j cogroup take 1", joined_Vbeta_i_j[1]
    #print "map ", map(lambda (x,y): (x, (y for y in list(y[0]))), joined_Vbeta_i_j)
    # m1_Vbeta_inv_Sigmabeta_j_draw : iter, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j
    m1_Vbeta_inv_Sigmabeta_j_draw = map(lambda (x,y): get_m1_Vbeta_inv_Sigmabeta_j_draw(list(y)), joined_Vbeta_i_j) 
    #print " m1_Vbeta_inv_Sigmabeta_j_draw Take 1: ", m1_Vbeta_inv_Sigmabeta_j_draw[1]
    #print " m1_Vbeta_inv_Sigmabeta_j_draw Count: ", len(m1_Vbeta_inv_Sigmabeta_j_draw)
    #sc.parallelize(m1_Vbeta_inv_Sigmabeta_j_draw).saveAsTextFile("hdfs://sandbox:9000/m1_Vbeta_inv_Sigmabeta_j_draw.txt")
    m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2 = sc.parallelize(m1_Vbeta_inv_Sigmabeta_j_draw).keyBy(lambda (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j): (hierarchy_level2)) 
    
    ##-- m1_beta_mu_j : Compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  
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
     
    ## -- m1_beta_mu_j_draw : Draw beta_mu from mvnorm dist'n.  Get back J vectors of beta_mu, one for each J.
    ## Simply creates a join on  m1_beta_mu_j and  m1_Vbeta_inv_Sigmabeta_j_draw (the RDD keyby h2 equivalent is m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2 )
    ## extracts iter, hierarchy_level2 and beta_draw(beta_mu_j, Sigmabeta_j)
    joined_m1_beta_mu_j_with_m1_Vbeta_inv_Sigmabeta_j_draw_rdd = m1_beta_mu_j.cogroup(m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2)
    m1_beta_mu_j_draw = joined_m1_beta_mu_j_with_m1_Vbeta_inv_Sigmabeta_j_draw_rdd.map(get_beta_draw).keyBy(lambda (iter, hierarchy_level2, beta_mu_j_draw): hierarchy_level2)
    # count of 5    
    #print "count m1_beta_mu_j_draw", m1_beta_mu_j_draw.count()
    # take 1 of <h2> => (iter, h2, beta_mu_j_draw)    
    #print "take 1 m1_beta_mu_j_draw", m1_beta_mu_j_draw.take(1)
    
    ## -- Compute Vbeta_i
    ## Uses a join of m1_d_array_agg_constants & m1_Vbeta_inv_Sigmabeta_j_draw
    ## m1_d_array_agg_constants is RDD of tuples h2,h1,xtx,xty
    ## m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2 is RDD of (key, Value)::(h2 => (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j))
    m1_d_array_agg_constants_key_by_h2 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2))
    #print "table 1 :",m1_d_array_agg_constants_key_by_h2.take(1)
    #print "table 1 count :",m1_d_array_agg_constants_key_by_h2.count()
    #print "table 2: ", m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2.take(1)
    #print "table 2 count 135: ",m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2.count()
    joined_m1_d_array_agg_constants_with_m1_Vbeta_inv_Sigmabeta_j_draw = m1_d_array_agg_constants_key_by_h2.cogroup(m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2)
    # count of 5    
    #print "count joined_m1_d_array_agg_constants_with_m1_Vbeta_inv_Sigmabeta_j_draw ", joined_m1_d_array_agg_constants_with_m1_Vbeta_inv_Sigmabeta_j_draw.count()
    #print "take 1 m1_beta_mu_j_draw", joined_m1_d_array_agg_constants_with_m1_Vbeta_inv_Sigmabeta_j_draw.take(1)
    # m1_Vbeta_i : iter, h2, h1, Vbeta_i
    m1_Vbeta_i = joined_m1_d_array_agg_constants_with_m1_Vbeta_inv_Sigmabeta_j_draw.map(lambda (x,y): (x, get_Vbeta_i(y)))
    #print "count m1_Vbeta_i", m1_Vbeta_i.count()
    #print "take 1 m1_Vbeta_i", m1_Vbeta_i.take(1)
      
    # -- Compute beta_i_mean
    m1_Vbeta_i_unified = sc.parallelize(m1_Vbeta_i.values().reduce(add))
    m1_Vbeta_i_keyby_h2_h1 = m1_Vbeta_i_unified.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i): (hierarchy_level2, hierarchy_level1))
    m1_d_array_agg_constants_key_by_h2_h1 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2, h1))
    # JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1 of tuples : hierarchy_level2, hierarchy_level1, Vbeta_i,xty
    JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1 = m1_Vbeta_i_keyby_h2_h1.cogroup(m1_d_array_agg_constants_key_by_h2_h1).map(lambda (x,y): (list(y[0])[0][1],list(y[0])[0][2], list(y[0])[0][3], list(y[1])[0][3]))
    JOINED_part_1_by_keyBy_h2 = JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1.keyBy(lambda (hierarchy_level2, hierarchy_level1, Vbeta_i, xty): hierarchy_level2)
    #print "table outer count ",  JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1.count()
    #print "table outer take 1",  JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1.take(1)     
    # Following is h2 ->(resIter1, resIter2) <=> where 
    # resIter1 is (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j)
    # resIter2 is (iter, hierarchy_level2, beta_mu_j_draw)
    JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw = m1_Vbeta_inv_Sigmabeta_j_draw_rdd_key_h2.cogroup(m1_beta_mu_j_draw).map(lambda (x,y): (x, list(y[0])[0][0], list(y[0])[0][3], list(y[1])[0][2]))
    JOINED_part_2_by_keyBy_h2 = JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.keyBy(lambda (hierarchy_level2, i, Vbeta_inv_j_draw, beta_mu_j_draw): hierarchy_level2)
    #print "take 1 ", JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.take(1) 
    #print "count 1 ", JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.count()
    m1_beta_i_mean = JOINED_part_1_by_keyBy_h2.cogroup(JOINED_part_2_by_keyBy_h2).map(lambda (x,y): (x, get_beta_i_mean(y)))
    #beta_i_mean = JOINED_part_1_by_keyBy_h2.cogroup(JOINED_part_2_by_keyBy_h2).map(lambda (x,y): (x, list(y[0]),list(y[1])))
    #print "beta_i_mean take ", m1_beta_i_mean.take(1) 
    #print "beta_i_mean count ", m1_beta_i_mean.count()
    
    #-- compute m1_beta_i_draw by  Draw beta_i from mvnorm dist'n
    # using m1_Vbeta_i_keyby_h2_h1 : h2, h1 => (i, hierarchy_level2, hierarchy_level1, Vbeta_i)
    # & parallelizing  beta_i_mean using h2, h1
    m1_beta_i_draw_unified = sc.parallelize(m1_beta_i_mean.values().reduce(add))
    m1_beta_i_mean_keyBy_h2_h1 = m1_beta_i_draw_unified.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, beta_i_mean): (hierarchy_level2, hierarchy_level1))
    # JOINED_m1_beta_i_mean_WITH_m1_Vbeta_i
    # m1_beta_i_draw : (iter, h2, h1, beta_i_draw)
    m1_beta_i_draw = m1_beta_i_mean_keyBy_h2_h1.cogroup(m1_Vbeta_i_keyby_h2_h1).map(lambda (x,y): (list(y[0])[0][0], x[0], x[1], gu.beta_draw(list(y[0])[0][3], list(y[1])[0][3])))
    #print "beta_i_mean take ", m1_beta_i_draw.take(1) 
    #print "beta_i_mean count ", m1_beta_i_draw.count() # 135

    m1_beta_i_draw_group_by_h2_h1 = m1_beta_i_draw.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, beta_i_draw): (hierarchy_level2, hierarchy_level1))
    m1_d_array_agg_key_by_h2_h1 = m1_d_array_agg.keyBy(lambda (keys, x_matrix, y_array, hierarchy_level2, hierarchy_level1) : (keys[0], keys[1]))
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
    foo2 = foo.map(lambda (x, y): get_sum_beta_i_draw_x2(y)).keyBy(lambda (hierarchy_level2, hierarchy_level1, iteri, ssr, m1_d_count_grpby_level2_b): (hierarchy_level2, iteri, m1_d_count_grpby_level2_b))
    #print "foo2 : 5 : ", foo2.take(1)
    #print "foo2 : 5 : ", foo2.count()
    
    #foo3 = foo2.groupByKey().map(lambda (x, y): get_s2(list(y)))
    # iteri, hierarchy_level2, m1_d_count_grpby_level2_b, s2
    m1_s2 = foo2.groupByKey().map(lambda (x, y): get_s2(list(y)))
    print "m1_s2 : 5 : ", m1_s2.take(1)
    print "m1_s2 : 5 : ", m1_s2.count() 
    
    ### -- Draw h from gamma distn.  Note that h=1/(s^2)
    ## from iteri, hierarchy_level2, m1_d_count_grpby_level2_b, s2
    ## m1_h_draw = iteri, h2, h_draw
    m1_h_draw = m1_s2.map(get_h_draw)
    print "m1_h_draw : 5 : ", m1_h_draw.take(1)
    print "m1_h_draw : 5 : ", m1_h_draw.count() 
    ##m1_h_draw.saveAsTextFile("hdfs://sandbox:9000m1_h_draw.txt")
    ##################################################################################Gibbs ###
    s = 2
    m1_h_draw_filter_by_iteration = m1_h_draw.filter(lambda (iteri, h2, h_draw): iteri == s - 1 ).keyBy(lambda (iteri, h2, h_draw): h2)
    #print "m1_h_draw_filter_by_iteration", m1_h_draw_filter_by_iteration.collect()
    m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration = sc.parallelize(m1_Vbeta_inv_Sigmabeta_j_draw).filter(lambda (iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): iteri == s - 1 ).keyBy(lambda (iteri, h2, n1, Vbeta_inv_j_draw, Sigmabeta_j): h2)
    # filter m1_h_draw taking only |s| -1 into account
    m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration_join_m1_h_draw_filter_by_iteration = m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration.cogroup(m1_h_draw_filter_by_iteration)
    
    # Creating a new structure joined_simplified : iteri, h2, Vbeta_inv_j_draw, h_draw, which is a simplified version of what the original structure looks.
    joined_simplified = m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration_join_m1_h_draw_filter_by_iteration.map(lambda (x,y): (list(y[0])[0][0], x, list(y[0])[0][3], list(y[1])[0][2]))
    joined_simplified_key_by_h2 = joined_simplified.keyBy(lambda (iteri, h2, Vbeta_inv_j_draw, h_draw): h2)
    ## m1_d_array_agg_constants is RDD of tuples h2, h1, xtx, xty
    ## joined_simplified is RDD of tuples h2 -> iteri, h2, Vbeta_inv_j_draw, h_draw 
    # join the m1_d_array_agg_constants_key_by_h2 with join_simplified
    m1_d_array_agg_constants_key_by_h2 = m1_d_array_agg_constants.keyBy(lambda (h2, h1, xtx, xty): (h2))  
    m1_d_array_agg_constants_key_by_h2_join_joined_simplified = m1_d_array_agg_constants_key_by_h2.cogroup(joined_simplified_key_by_h2)
    #print "count and take 1", m1_d_array_agg_constants_key_by_h2_join_joined_simplified.count(), m1_d_array_agg_constants_key_by_h2_join_joined_simplified.take(1)
    # following two lines of mapped RDD are for testing : m1_d_array_agg_constants_key_by_h2_join_joined_simplified
    #m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped = m1_d_array_agg_constants_key_by_h2_join_joined_simplified.map(lambda (x,y): (x, list(y[0])[0], list(y[1])[0]))
    #print "m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped", m1_d_array_agg_constants_key_by_h2_join_joined_simplified_mapped.take(1)
    
    # compute next values of m1_Vbeta_i : (h2, [(count, hierarchy_level2, hierarchy_level1, Vbeta_i)])
    m1_Vbeta_i_next = m1_d_array_agg_constants_key_by_h2_join_joined_simplified.map(lambda (x,y): (x, get_Vbeta_i_next(y, s)))
    # the Unified table is the actual table that reflects all the rows of m1_Vbeta_i in correct format.
    m1_Vbeta_i_unified = m1_Vbeta_i_unified.union(sc.parallelize(m1_Vbeta_i_next.values().reduce(add)))
    #print "count  m1_Vbeta_i_unified   ", m1_Vbeta_i_unified.count()
    #print "take 1 m1_Vbeta_i_unified ", m1_Vbeta_i_unified.take(1)
    m1_Vbeta_i_keyby_h2_h1 = m1_Vbeta_i_unified.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i): (hierarchy_level2, hierarchy_level1))

   
    ### insert into beta_i_mean
    m1_Vbeta_i_unified_filter_iter_s = m1_Vbeta_i_unified.filter(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i) : i == s)
    m1_Vbeta_i_keyby_h2_h1 = m1_Vbeta_i_unified_filter_iter_s.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, Vbeta_i): (hierarchy_level2, hierarchy_level1))
    #print "count  m1_Vbeta_i_keyby_h2_h1   ", m1_Vbeta_i_keyby_h2_h1.count()
    #print "take 1 m1_Vbeta_i_keyby_h2_h1 ", m1_Vbeta_i_keyby_h2_h1.take(1)    
    #  JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1 of tuples : hierarchy_level2, hierarchy_level1, Vbeta_i,xty 
    # following is the foo of computer beta_i_mean
    JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1 = m1_Vbeta_i_keyby_h2_h1.cogroup(m1_d_array_agg_constants_key_by_h2_h1).map(lambda (x,y): (list(y[0])[0][1],list(y[0])[0][2], list(y[0])[0][3], list(y[1])[0][3]))
    JOINED_part_1_by_keyBy_h2 = JOINED_m1_Vbeta_i_keyby_h2_h1_WITH_m1_d_array_agg_constants_key_by_h2_h1.keyBy(lambda (hierarchy_level2, hierarchy_level1, Vbeta_i, xty): hierarchy_level2)
    
    # m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration joined with m1_beta_mu_j_draw_by_iteration
    # m1_beta_mu_j_draw = hierarchy_level2 -> (iter, hierarchy_level2, beta_mu_j_draw)
    m1_beta_mu_j_draw_by_iteration = m1_beta_mu_j_draw.filter(lambda (x,y): y[0] == s - 1)
    #print "count  m1_beta_mu_j_draw_by_iteration   ", m1_beta_mu_j_draw_by_iteration.count()
    #print "take 1 m1_beta_mu_j_draw_by_iteration ", m1_beta_mu_j_draw_by_iteration.take(1)
    # Following is computed from h2 ->(resIter1, resIter2) <=> where 
    # resIter1 is (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j)
    # resIter2 is (iter, hierarchy_level2, beta_mu_j_draw)
    # strucuture is  ( h2, iter, Vbeta_inv_j_draw, beta_mu_j_draw )
    JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw = m1_Vbeta_inv_Sigmabeta_j_draw_by_iteration.cogroup(m1_beta_mu_j_draw_by_iteration).map(lambda (x,y): (x, list(y[0])[0][0], list(y[0])[0][3], list(y[1])[0][2])).keyBy(lambda (hierarchy_level2, iteri, Vbeta_inv_j_draw, beta_mu_j_draw): hierarchy_level2)
    # Cogroup above structure h2 -> h2, iter, Vbeta_inv_j_draw, beta_mu_j_draw with m1_h_draw_filter_by_iteration,  h2 -> (iteri, h2, h_draw)
    JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw_join_WITH_h_draw = JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.cogroup(m1_h_draw_filter_by_iteration).map(lambda (x, y): (list(y[0])[0][0], list(y[1])[0][2], list(y[0])[0][2], list(y[0])[0][3]))
    JOINED_part_2_by_keyBy_h2 = JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw_join_WITH_h_draw.keyBy(lambda (hierarchy_level2, h_draw, Vbeta_inv_j_draw, beta_mu_j_draw): hierarchy_level2)
    #print "take 1 ", JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.take(1) 
    #print "count 1 ", JOINED_m1_Vbeta_inv_Sigmabeta_j_draw_WITH_m1_with_m1_beta_mu_j_draw.count()
    m1_beta_i_mean_next = JOINED_part_1_by_keyBy_h2.cogroup(JOINED_part_2_by_keyBy_h2).map(lambda (x,y): (x, get_beta_i_mean(y)))
    # the Unified table is the actual table that reflects all rows of m1_beta_i_draw in correct format.
    m1_beta_i_draw_unified = m1_beta_i_draw_unified.union(sc.parallelize(m1_beta_i_mean_next.values().reduce(add)))
    print "count  m1_Vbeta_i_unified   ", m1_beta_i_draw_unified.count()
    print "take 1 m1_Vbeta_i_unified ", m1_beta_i_draw_unified.take(1)
    m1_beta_i_mean_keyBy_h2_h1 = m1_beta_i_draw_unified.keyBy(lambda (i, hierarchy_level2, hierarchy_level1, beta_i_mean): (hierarchy_level2, hierarchy_level1))
    

    
    

    
    
    
    
