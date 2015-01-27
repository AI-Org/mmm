import numpy as np
def invokeWishart():
    import os
    return os.popen('Rscript wishart.R ').read()

df = 14
phi = np.matrix()
a = np.matrix('1 .3; .3 1')
print a.astype('str')
print dim(a)
#draw <- rwish(3, matrix(c(1,.3,.3,1),2,2))
invokeWishart()

