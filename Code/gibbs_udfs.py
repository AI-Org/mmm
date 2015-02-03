import numpy as np

# Function to compute median of any array.
# It takes "anyarray" of type numpy array. 
def final_median(anyarray):
    return np.median(anyarray)

# Function to add two matrixes
def matrix_add_plr(matrix1, matrix2):
    return np.add(matrix1, matrix2)

# Function to diagonalize 1d Array
def matrix_diag_plr(anyarray):
    return np.diag(anyarray)

# Function to add a diagonal matrix created from a scalar value to any matrix
def matrix_add_diag_plr(matrix, value):
    a = np.zeros(matrix.shape, float)
    np.fill_diagonal(a, value)
    return np.add(matrix, a)

# Function to multiply a scalar to a matrix
def matrix_scalarmult_plr(matrix, value):
    #print "multiply matrix : ", matrix ," with value ", value
    return np.multiply(matrix, value)

# Function to compute var-cov matrix for the pooled model, as defined in Equation 7.27 of Koop pp.156-157
# beta_i is a float8[] matrix or numpy.array or numpy.matrix
# beta_mu is anpther float8[] matrix or numpy.array or numpy.matrix
# according to Koop pp. 157 sigma(Beta_i) is a k-vector containing the sums of the elements of Beta_i
# hance we convert beta_i_diff to mat before multiplying

def Vbeta_i_mu(beta_i, beta_mu):
    beta_i_diff = np.subtract(beta_i, beta_mu)
    Vbeta_i_mu = np.multiply(beta_i_diff, np.mat(beta_i_diff).transpose())
    return Vbeta_i_mu


# rwish generates one random draw from the distribution

def Vbeta_inv_draw(dof, sigma):
    # import a lib MCMCpack and return rwish(arg1,arg2)
    import wishart as rwish
    wishart = rwish.Wishart(dof)
    return wishart.sample_wishart(dof, sigma)

# Function to compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  
# This function allows for user-specified priors on the coefficients.  
# For use at highest level of the hierarchy.  (7.27) of Koop pp.157.

def beta_mu_prior(Sigmabeta_j, Vbeta_inv_j_draw, sum_coef_j, coef_means_prior_array, coef_precision_prior_array):
    #beta_mu<- arg1%*%(arg2%*%arg3+as.matrix(arg4*arg5))
    # used for computing the mean for beta_draws,
    # should be of order size of variables i.e 13
    # mat1 is a dot product of two arrays
    mat1 = np.dot(coef_means_prior_array, coef_precision_prior_array)
    #mat2 is a matrix multiplication product of inv_j_draw and sum_coeff_j
    #sum_coef_j = sum_coef_j.reshape(13,1)
    mat2 = np.dot(Vbeta_inv_j_draw, sum_coef_j)
    mat3 = np.add(mat2, np.mat(mat1))
    # NOTE : for the return value to be one D the sum_coef_j should be a 1 D matrix.
    return np.dot(Sigmabeta_j, mat3.T)


# beta_draws are samples from mvrnprm or multivariate normal distribution.
# it relies on MASS library in the original implementation However, we will be using
# numpy and its random package to perform the same operation

def beta_draw(mean, cov):
    # mean should be a 1 D array of means of variables
    # cov should be 2 D array of 
    return np.random.multivariate_normal(np.matrix(mean).getA1(), cov, 1)

# Function to compute Vbeta_i, as defined in Equation (7.25) of Koop pp.156.   
# Only computed at lowest level of the hierarchy (i.e. the level that "mixes" directly with the data, namely X'X).
# The solve(mat1*%*mat2 + mat3) is finding the inverse of A where A = mat1 %*% mat2 + mat3

def Vbeta_i(mat1, mat2, mat3):
    mat1_r = np.dot(mat1, mat2)
    matr = np.add(mat1_r, mat3)
    return np.linalg.inv(matr)   

# Function to compute beta_i_mean, as defined in (7.25) of Koop pp.156.
# Only computed at lowest level of the hierarchy (i.e. the level that "mixes" directly with the data, namely X'y).

def beta_i_mean(Vbeta_i, value, xty, Vbeta_i_inv_draw, beta_mu_j_draw):
    #def beta_i_mean(mat1, value, mat2, mat3, mat4):
    # error was mat_r1 is 1 X 13 instead of 13 X 1 but with T it will be not
    mat_r1 = np.dot(np.matrix(Vbeta_i_inv_draw), np.matrix(beta_mu_j_draw).getA1()).T
    # mat_r2 is 13 X 1
    mat_r2 = np.dot(value, xty)
    # mat_r3 is 13 X 1 == 13 X 13 + 13 X 1
    mat_r3 = np.dot(Vbeta_i, np.add(mat_r2, mat_r1))
    return mat_r3

# Function to draw h from gamma dist'n, as defined in (7.28) of Koop pp.157. 
# rate is an alternate way to specify the scale, shape of the gamma distribution
# numpy.random.gamma(shape, scale=1.0, size=None) is equivalent to rgamma(n, shape, rate = 1, scale = 1/rate)

def h_draw(m, v):
    shape_g = v/2
    rate_g = v / (2 * m)
    scale_g = 1/ rate_g
    return np.random.gamma(shape_g, scale_g, 1 )
     
# Function to draw random array sample of p elements from the uniform(-1,1) dist'n
# numpy.random.uniform(low=0.0, high=1.0, size=None)
     
def initial_vals_random(p):
    return np.random.uniform(-1,1,p)
    




    

