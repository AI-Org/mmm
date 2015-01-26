"""
@author: ssoni

Gibbs transformations are applied to the data structures created during Gibbs init 
and Gibbs iterative algorithm
"""
import numpy as np
import gibbs_udfs as gu

#'p_var' = Number of explanatory variables in the model, including the intercept term.
p_var = 14
accum = 0
df1_var = 15
 
# Looks like there was a one in row or
# column somewhere in the computations that was missed out. I was to proceed without any
# corrections at this point of time and revisit this problem again.
coef_precision_prior_array_var = [1,1,1,1,1,1,1,1,1,1,1,1,1,1]
coef_means_prior_array_var = [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
sample_size_deflator = 1

# OPTIMIZATION 4
# Following function should work on each partition's values
# recObj represents one dataobject/dataset coming from one partition which can 
# be either key by h2 or h2_h1

## d.reduceByKey(create_x_matrix_y_array).take(1) 
# where each tuple is index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)
#def create_x_matrix_y_array(tuple1,tuple2):
#    """
#       Take an iterable of records, where the key corresponds to a certain age group
#       Create a numpy matrix and return the shape of the matrix
#    """
#    import numpy as np
#    # challenge here is ValueError: zero-dimensional arrays cannot be concatenated
#    #tup[1] = 2 >>> np.array((tup)[1]) array(2) which is a zero dimention array
#    #(tup[1],) = (2) >>> np.array(((tup)[1],)) array([2]) which is a one dimension array
#    y_array = []
#    if len(tuple1)
#    if type(tuple1) is 'tuple' and type(tuple2) is 'tuple':
#        y1 = tuple((tuple1[4],))
#        y2 = tuple((tuple2[4],))
#        y_array = list(y1+y2)
#    elif type(tuple1) is 'tuple' and type(tuple2) is 'list':
#        y1 = tuple((tuple1[4],))
#        y_array = tuple2.append(y1)
#    elif type(tuple1) is list and type(tuple2) is list:
#        y1 = tuple1[4]
#        y2 = tuple2[4]
#        y_array = y1 + y2 
#    elif type(tuple2) is 'tuple' and type(tuple1) is 'list':
#        y2 = tuple((tuple2[4],))
#        y1 = tuple1
#        y_array = y1.append(y2)
#    else:
#        y_array = [type(tuple1), type(tuple2)]
#    return y_array
#
#
#d.reduceByKey(create_x_matrix_y_array).take(1)
#    
#    if type(tuple1) == 'tuple' and type(tuple2) == 'tuple':
#        y1 = np.array(tuple((tuple1[4],))).astype(float)
#        y2 = np.array(tuple((tuple2[4],))).astype(float)
#        y_array = np.concatenate((y1,y2), axis = 0)
#    if type(tuple1) == 'tuple' and type(tuple2) == 'list':
#        y1 = np.array(tuple((tuple1[4],))).astype(float)
#        y_array = tuple2.append(y1)
#        #y_array = np.concatenate((y1,y2), axis = 0)
#    if type(tuple1) == 'list' and type(tuple2) == 'list':
#        y1 = tuple1
#        y2 = tuple2
#        y_array = y1.append(y2) 
#    if type(tuple2) == 'tuple' and type(tuple1) == 'list':
#        y2 = np.array(tuple((tuple2[4],))).astype(float)
#        y1 = tuple1
#        y_array = y1.append(y2)
#    return y_array
#
#
#
#
##    tup1 = (1,)
##    x_matrix1 = np.matrix(tup1+tuple1[5:18]).T.astype(float)
##    x_matrix2 = np.matrix(tup1+tuple2[5:18]).T.astype(float)
##    x_matrix1 * x_matrix2
##    x_matrix = np.hstack(x_matrix1, x_matrix2).T
##    return (y_array, x_matrix)
## d.groupByKey().map(create_x_matrix_y_array).take(1)    
#def create_x_matrix_y_array(recObj):
#    """
#       Take an iterable of records, where the key corresponds to a certain age group
#       Create a numpy matrix and return the shape of the matrix
#       #recObj is of the form of [key, <Iterable of all values tuples>]
#    """
#    import numpy
#    keys = recObj[0]
#    recIter = recObj[1]
#    mat = numpy.matrix([r for r in recIter])
#    x_matrix = mat[:,5:18].astype(float)
#    x_matrix = numpy.append([[1 for _ in range(0,len(x_matrix))]], x_matrix.T,0).T
#    y_array = mat[:,4].astype(float)
#    hierarchy_level2 =  mat[:,2]
#    hierarchy_level1 = mat[:,1]
#    return (keys, x_matrix, y_array, hierarchy_level2[1,0], hierarchy_level1[1,0])
    
def create_x_matrix_y_array(recObj):
    """
       Take an iterable of records, where the key corresponds to a certain age group
       Create a numpy matrix and return the shape of the matrix
       #recObj is of the form of [<all values tuples>]
    """
    import numpy
    recIter = recObj
    keys = recObj[0] # partition value
    recIter = recObj[1]
    mat = numpy.matrix([r for r in recIter])
    x_matrix = mat[:,5:18].astype(float)
    x_matrix = numpy.append([[1 for _ in range(0,len(x_matrix))]], x_matrix.T,0).T
    y_array = mat[:,4].astype(float)
    hierarchy_level2 =  mat[:,2]
    hierarchy_level1 = mat[:,1]
    #return (keys, x_matrix, y_array, hierarchy_level2[1,0], hierarchy_level1[1,0])
    return (keys, x_matrix, y_array, hierarchy_level2[1,0], hierarchy_level1[1,0])


def create_xtx_matrix_xty(obj):
    import numpy
    #recObj is of the form of [key, <Iterable of all values tuples>]
    #keys = obj[0] # partition value
    x_matrix = obj[1]
    x_matrix_t = numpy.transpose(x_matrix)
    xt_x = x_matrix_t * x_matrix
    y_matrix = obj[2]
    xt_y = x_matrix_t * y_matrix
    hierarchy_level2 =  obj[3]
    hierarchy_level1 = obj[4]
    # h2, h1, xtx, xty
    return (hierarchy_level2,hierarchy_level1, xt_x, xt_y)

def get_d_childcount(obj):
    keyBy_h2_to_h1 = obj.map(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).groupByKey()
    # returns DS with key hierarchy_level2 and value <hierarchy_level2, n1>
    return keyBy_h2_to_h1.map(lambda (x,iter): (x, sum(1 for _ in set(iter))))


# as per the level 2 in integer mode
def get_d_childcount_mod(obj):
    keyBy_h2_to_h1 = obj.map(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).groupByKey()
    # returns DS with key hierarchy_level2 and value <hierarchy_level2, n1>
    return keyBy_h2_to_h1.map(lambda (x,iter): (int(str(x)[0]), sum(1 for _ in set(iter))))


def get_d_count_grpby_level2(obj):
    keyBy_h2_week = obj.map(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, week))
    return keyBy_h2_week
    #return keyBy_h2.map(lambda (x,iter): (x, sum(1 for _ in set(iter))))

def get_ols_initialvals_beta_i(obj):
    from sklearn import linear_model
    regr = linear_model.LinearRegression()
    # fit x_array, y_array
    regr.fit(obj[1], obj[2])
    # returns h2, h1, regr.coef_
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
def get_Vbeta_i_mu_coeff_i_coeff_j(list_coeff_i, coeff_j):
    #<h2,list<h1,h2,coef_i>,coef_j>
    Vbeta_i_mu_ar = []
    for r in list_coeff_i:
        values_array_i = r[2]    
        Vbeta_i_mu_ar.append(gu.Vbeta_i_mu(values_array_i, coeff_j))
    return Vbeta_i_mu_ar


def get_Vbeta_j_mu(y):
    # now y is an ResultIterable object pointing to a collection of arrays
    # where each array has a structure like <h2,list<h1,h2,coef_i>,coef_j>
    result_Iterable_list = list(y)[0]
    Vbeta_i_mu_ar = get_Vbeta_i_mu_coeff_i_coeff_j(result_Iterable_list[1], result_Iterable_list[2])
    # one can also obtain Vbeta_i_mu_sum as  map(lambda (x,y): (x, sum(fun(list(y)))), joined_i_j_rdd.take(1))
    # corresponding to each one of the h2 level
    Vbeta_i_mu_sum = sum(Vbeta_i_mu_ar)
    Vbeta_j_mu = gu.matrix_add_diag_plr(Vbeta_i_mu_sum, p_var)
    # iter, hierarchy_level2, Vbeta_j_mu
    return result_Iterable_list[0], Vbeta_j_mu
    
def get_Vbeta_j_mu_next(y):
    # OPTI now y is an ResultIterable object pointing RI1, RI2
    # OPTI where RI1 is a collection of tuples with h2, h1, beta_i_draw
    # OPTI and RI2 is a single tuple iter, hierarchy_level2, beta_mu_j_draw
    # y is tuple of as I put a map on cogroup h2, <all of beta_i_draw>, beta_mu_j_draw
    beta_mu_j_draw = list(y)[0][2]
    h2 = list(y)[0][0]
    Vbeta_i_mu_ar = []
    for rec in list(y)[0][1]:
        # rec : s, h2, h1, beta_i_draw
        beta_i_draw = rec[3]
        Vbeta_i_mu_ar.append(gu.Vbeta_i_mu(beta_i_draw, beta_mu_j_draw))
    Vbeta_j_mu = gu.matrix_add_diag_plr(sum(Vbeta_i_mu_ar) ,p_var) 
    return h2, Vbeta_j_mu

def get_m1_Vbeta_j_mu_pinv(obj):
    import numpy as np
    import nearPD as npd
    global df1_var
    seq = obj[0]
    hierarchy_level2 = obj[1][0]
    Vbeta_j_mu = obj[1][1]
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
    temp = gu.matrix_scalarmult_plr(Vbeta_inv_j_draw, n1) # (14 X 14)
    temp_add = gu.matrix_scalarmult_plr(temp, gu.matrix_diag_plr(coef_precision_prior_array))
    return np.linalg.pinv(temp_add)
    
def get_m1_Vbeta_inv_Sigmabeta_j_draw(lst):
    global coef_precision_prior_array_var
    #y[0][0], y[0][1], y[1][1] , y[0][2], pinv_Vbeta_inv_Sigmabeta_j_draw(y[0][2], y[1][1], coef_precision_prior_array_var)
    #or seq from m1_Vbeta_j_mu_pinv
    iter = 1 
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
    
def get_m1_Vbeta_inv_Sigmabeta_j_draw_next(y):
    global coef_precision_prior_array_var
    # y has structure : s, h2, Vbeta_inv_j_draw, n1
    iteri = y[0]
    h2 = y[1]
    Vbeta_inv_j_draw = y[2]
    n1 = y[3]
    return (iteri, h2, n1, Vbeta_inv_j_draw, pinv_Vbeta_inv_Sigmabeta_j_draw(Vbeta_inv_j_draw, n1, coef_precision_prior_array_var))


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
        Vbeta_inv_j_draw = r[3]
        Sigmabeta_j = r[4]
    for r in obj[1][1]:
        sum_coef_j = r[0]
    beta_mu_j = gu.beta_mu_prior(Sigmabeta_j, Vbeta_inv_j_draw, sum_coef_j, coef_means_prior_array_var, coef_precision_prior_array_var)
    return (iteri, hierarchy_level2, beta_mu_j)

def add_coeff_j(hierarchy_level2, iterable_object):
    # where each iterable is like (hierarchy_level2, array[[]] of dim 1X13)
    # Changed in OPTIMIZATIONS each iterable is like (hierarchy_level2, h1, array[[]] of dim 1X13)
    array_list = []
    for r in iterable_object:
        array_list.append(r[2])
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
        Vbeta_inv_j_draw = r[3]
        Sigmabeta_j = r[4]
    return (iteri, key, gu.beta_draw(beta_mu_j, Sigmabeta_j), Vbeta_inv_j_draw)
    
def pinv_Vbeta_i(xtx, Vbeta_inv_j_draw, h_draw):
    return gu.matrix_add_plr(gu.matrix_scalarmult_plr(xtx, h_draw), Vbeta_inv_j_draw)    
    #return (type(xtx), type(Vbeta_inv_j_draw))
    
def get_Vbeta_i(obj):
    # key is hierarchy_level2 and 
    # cogrouped_iterable_object is <W1,W2>
    # where W2 is a ResultIterable having hierarchy_level2 => (iter, hierarchy_level2, n1, Vbeta_inv_j_draw, Sigmabeta_j))
    for r in obj[0]:
        h2 = r[1]
        Vbeta_inv_j_draw = r[3]
    rows = []
    #count = 1
    # obj[0] where W1 is a ResultIterable having obj[1][0]=hierarchy_level2, obj[1][1]=hierarchy_level1, xtx, xty
    for r in obj[1]:
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
    
def get_beta_i_mean(y):
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
    # hierarchy_level2, hierarchy_level1,iteri, ssr ,m1_d_count_grpby_level2_b
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

# returns extended beta_draw values pertaining to each driver combination of h2, h1 and 
# dependable variable, returned structure is to be used in summary functions
# iter, h2, h1, beta_Draw, x_array_driver, hierarchy_level2_hierarchy_level1_driver  
def get_beta_i_draw_long(x):
    # x : s, h2, h1, beta_draw 
    rows = []
    s = x[0]
    h2 = x[1]
    h1 = x[2]
    beta_draw = x[3]
    # where each row : s, h2, h1, beta_draw[i], x_array[i], h2_h1_driver
    x_array = ['1', 'x1','x2','x3','x4','x5','x6','x7','x8','x9','x10','x11','x12','x13']
    for i in range(0,len(x_array)):
        hierarchy_level2_hierarchy_level1_driver = h2+"-:-"+h1+"-:-"+ x_array[i]
        row = (s, h2, h1, beta_draw[:,i][0], x_array[i], hierarchy_level2_hierarchy_level1_driver)
        rows.append(row)
    return rows
    
def compute_se_sa_i_avg_sa_i(y, raw_iters, burn_in):
    import numpy as np
    # s, h2, h1, beta_draw[i], x_array_i, h2_h1_driver
   
    beta_i_draw_array = []
    for row in y:
        beta_i_draw_array.append(row[3])
    avg_sa_i = np.average(beta_i_draw_array)
    stddev_samp = np.std(beta_i_draw_array)
    se_sa_i = stddev_samp/ np.sqrt(.1 * (raw_iters - burn_in))
    return (se_sa_i, avg_sa_i)
    
def compute_se_sc_i_avg_sc_i(y, raw_iters, burn_in):
    import numpy as np
    # s, h2, h1, beta_draw[i], x_array_i, h2_h1_driver
    
    beta_i_draw_array = []
    for row in y:
        beta_i_draw_array.append(row[3])
    avg_sc_i = np.average(beta_i_draw_array)
    stddev_samp = np.std(beta_i_draw_array)
    se_sc_i = stddev_samp/ np.sqrt((raw_iters - burn_in) - .6 * (raw_iters - burn_in))
    return (se_sc_i, avg_sc_i)
    
def get_cd_beta_i(se_sa_i, avg_sa_i, se_sc_i, avg_sc_i):
    cd_beta_i = (avg_sa_i-avg_sc_i)/(se_sa_i+se_sc_i)
    return cd_beta_i
    
    
    
        