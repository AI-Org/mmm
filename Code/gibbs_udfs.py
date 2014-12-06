from numpy import matrix
from numpy import median
import numpy as np

import numpy.random as npr
from numpy.linalg import inv, cholesky
from scipy.stats import chi2

# Function takes recObj is of the form of [key, <Iterable of all values tuples>]
# returns matrix from recIter objects and returns the shape of the matrix
def createMatrix(recObj):
    """
       Take an iterable of records, where the key corresponds to a certain age group
       Create a numpy matrix and return the shape of the matrix
    """
    recIter = recObj[1]
    #Each entry in recIter is an iterable of <values tuple>
    mat = matrix([r for r in recIter])
    return mat.shape

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
    return np.multiply(matrix, value)

# Function to compute var-cov matrix for the pooled model, as defined in Equation 7.27 of Koop pp.156-157
# beta_i is a float8[] matrix or numpy.array or numpy.matrix
# beta_mu is another float8[] matrix or numpy.array or numpy.matrix
# according to Koop pp. 157 sigma(Beta_i) is a k-vector containing the sums of the elements of Beta_i
# hance we convert beta_i_diff to mat before multiplying
def Vbeta_i_mu(beta_i, beta_mu):
    beta_i_diff = np.subtract(beta_i, beta_mu)
    Vbeta_i_mu = np.multiply(beta_i_diff, np.mat(beta_i_diff).transpose())
    return Vbeta_i_mu

## Same usage as mentioned by the function Vbeta_inv_draw
## usage wishartrand(nu, phi)
## where nu is degree of freedom
## phi is the matrix
## taken from
## https://gist.github.com/jfrelinger/2638485
def wishartrand(nu, phi):
    dim = phi.shape[0]
    chol = cholesky(phi)
    #nu = nu+dim - 1
    #nu = nu + 1 - np.arange(1,dim+1)
    foo = np.zeros((dim,dim))
    
    for i in range(dim):
        for j in range(i+1):
            if i == j:
                foo[i,j] = np.sqrt(chi2.rvs(nu-(i+1)+1))
            else:
                foo[i,j]  = npr.normal(0,1)
    return np.dot(chol, np.dot(foo, np.dot(foo.T, chol.T)))

# Function to draw Vbeta_inv from Wishart dist'n, as shown in Equation (7.25) of Koop pp.156-157
# rwish is random generation from the Wishart distribution from MCMCpack package
#  nu is Degrees of Freedom a scalar quantity i.e. v
#  phi is Inverse scale matrix (p X p) i.e S (pXp)
# The mean of a Wishart random variable with v degrees of freedom and inverse scale matrix S is vS.
# rwish generates one random draw from the distribution
def Vbeta_inv_draw(nu, phi):
    # import a lib MCMCpack and return rwish(arg1,arg2)
    return wishartrand(nu, phi)

# Function to compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  
# This function allows for user-specified priors on the coefficients.  
# For use at highest level of the hierarchy.  (7.27) of Koop pp.157.
def beta_mu_prior(arg1, arg2, arg3, arg4, arg5):
    #beta_mu<- arg1%*%(arg2%*%arg3+as.matrix(arg4*arg5))
    mat1 = np.dot(np.mat(arg4),np.mat(arg5))
    mat2 = np.dot(arg2, arg3)
    mat3 = np.add(mat1, mat2)
    return np.dot(arg1, mat3)


# beta_draws are samples from mvrnorm or multivariate normal distribution.
# it relies on MASS library in the original implementation However, we will be using
# numpy and its random package to perform the same operation
def beta_draw(mean, cov):
    return np.random.multivariate_normal(mean, cov, 1)

# Function to compute Vbeta_i, as defined in Equation (7.25) of Koop pp.156.   
# Only computed at lowest level of the hierarchy (i.e. the level that "mixes" directly with the data, namely X'X).
def Vbeta_i(val, mat1, mat2):
    return np.add(np.dot(val, mat1), mat2) 

# Function to compute beta_i_mean, as defined in (7.25) of Koop pp.156.
# Only computed at lowest level of the hierarchy (i.e. the level that "mixes" directly with the data, namely X'y).
def beta_i_mean(mat1, value, mat2, mat3, mat4):
    mat_r1 = np.dot(np.matrix(mat3), np.matrix(mat4))
    mat_r2 = np.dot(value, mat2)
    mat_r3 = np.dot(mat1, mat_r2)
    return mat_r3

# Function to draw h from gamma dist'n, as defined in (7.28) of Koop pp.157. 
# rate is an alternate way to specify the scale, shape of the gamma distribution
# numpy.random.gamma(shape, scale=1.0, size=None) is equivalent to rgamma(n, shape, rate = 1, scale = 1/rate)
def h_draw(m, v):
    shape_g = v/2
    rate_g = v / (2 * m)
    scale_g = 1/ rate
    return np.random.gamma(shape_g, scale_g, 1 )
     
# Function to draw random array sample of p elements from the uniform(-1,1) dist'n
# numpy.random.uniform(low=0.0, high=1.0, size=None)
def initial_vals_random(p):
    retusn np.random.uniform(-1,1,p)

    

