import rpy2.robjects as robjects
import numpy as np


np.random.seed(1)
a = np.matrix(('1 .3; .3 1'))
phi = 3 

data = robjects.r("""
#set.seed(1)
library(MCMCpack)
a = matrix(c(1,.3, .3, 1), 2, 2)
phi = 3
avg_ar <- rwish(phi, a)
for (i in 1:10000) {
    y <- rwish(phi, a)
    avg_ar <- c(avg_ar, y)
}
x <- mean(avg_ar)
""")

np.random.seed(1)
def sample_R(dof, sigma):
    import numpy as np
    from scipy import linalg
    """Returns a draw from the distribution - will be a symmetric positive definite matrix."""
    n = sigma.shape[0]
    A = np.zeros((n,n),dtype=np.float32) 
    #try:
    chol = linalg.cholesky(sigma, lower=True)
        #chol = numpy.linalg.cholesky(sigma)
    #except:
    #    print "Sigma as ", sigma
    #    print "eigs", np.all(np.linalg.eigvals(sigma) > 0)
    alpha = np.zeros((n,n),dtype=np.float32)
    
    for i in range(0, dof):
        #rnorm(Sigma.rows(), 1, 0, 1); with mean 1 and sd 0
        alpha = np.dot(chol, np.random.normal(0, 1, size=(n,dof)))
        A = np.add(A, np.dot(alpha, alpha.T))
    return A  

print sample_R(phi, a)     

print ""

np_ar = [sample_R(phi, a)]
for i in range(0, 10000):
    np_ar.append(sample_R(phi, a))

avg_np = np.average(np_ar)
avg_np_m = np.mean(np_ar)

print "average for R :", robjects.r["x"]
print "average for np :", avg_np
print "average for np :", avg_np_m

## Random sample from the one found on google website
import math
import random
import numpy as np
from scipy import linalg
import numpy.random

def sample(dof, sigma):
    """Returns a draw from the distribution - will be a symmetric positive definite matrix."""
    cholesky = linalg.cholesky(sigma)
    d = sigma.shape[0]
    a = numpy.zeros((d,d),dtype=numpy.float32)
    for r in xrange(d):
      if r!=0: a[r,:r] = numpy.random.normal(size=(r,))
      a[r,r] = math.sqrt(random.gammavariate(0.5*(dof-d+1),2.0))
    result_draw = numpy.dot(numpy.dot(numpy.dot(cholesky,a),a.T),cholesky.T)
    #print "result_draw" , result_draw
    return result_draw

np_ar_1 = [sample(phi, a)]
for i in range(0, 10000):
    np_ar_1.append(sample(phi, a))
    
avg_np = np.average(np_ar)
avg_np_m = np.mean(np_ar)

print "average for R :", robjects.r["x"]
print "average for np :", avg_np
print "average for np :", avg_np_m
    
avg_np_1 = np.average(np_ar_1)
avg_np_m_1 = np.mean(np_ar_1)

print "average for R :", robjects.r["x"]
print "average for np :", avg_np_1
print "average for np :", avg_np_m_1

## Random Samples from a third draw as numpy

def sample_wishart(dof, sigma):
    '''
    Returns a sample from the Wishart distn, conjugate prior for precision matrices.
    '''
    n = sigma.shape[0]
    chol = np.linalg.cholesky(sigma)
    
    # use matlab's heuristic for choosing between the two different sampling schemes
    if (dof <= 81+n) and (dof == round(dof)):
        #direct
        X = np.dot(chol,np.random.normal(size=(n,dof)))
    else:
        A = np.diag(np.sqrt(np.random.chisquare(dof - np.arange(0,n),size=n)))
        A[np.tri(n,k=-1,dtype=bool)] = np.random.normal(size=(n*(n-1)/2.))
        X = np.dot(chol,A)
    return np.dot(X,X.T)

np_ar_2 = [sample_wishart(phi, a)]
for i in range(0, 10000):
    np_ar_2.append(sample_wishart(phi, a))


    
avg_np_2 = np.average(np_ar_2)
avg_np_m_2 = np.mean(np_ar_2)

print "average for R :", robjects.r["x"]
print "average for np :", avg_np_2
print "average for np :", avg_np_m_2
