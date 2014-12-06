from numpy import matrix
from numpy import median
import numpy as np

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
# beta_i is a float8[] matrix
# beta_mu is another float8[] matrix
def Vbeta_i_mu(beta_i, beta_mu):
    beta_i_diff = np.subtract(beta_i, beta_mu)
    Vbeta_i_mu = np.multiply(beta_i_diff, beta_i_diff.transpose())
    return Vbeta_i_mu

# Function to draw Vbeta_inv from Wishart dist'n, as shown in Equation (7.25) of Koop pp.156-157
# rwish is random generation from the Wishart distribution from MCMCpack package
# floating_num is Degrees of Freedom a scalar quantity i.e. v
# float_matrix is Inverse scale matrix (p X p) i.e S (pXp)
# The mean of a Wishart random variable with v degrees of freedom and inverse scale matrix S is vS.
# rwish generates one random draw from the distribution
def Vbeta_inv_draw(floating_num, float_matrix):
    # import a lib MCMCpack and return rwish(arg1,arg2)


