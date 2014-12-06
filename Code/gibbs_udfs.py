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


