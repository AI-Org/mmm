# -*- coding: utf-8 -*-
"""
Created on Fri Dec 12 16:47:37 2014

@author: ssoni
"""

from pyspark import SparkContext


if __name__ == "__main__":
    sc = SparkContext(appName="GibbsSampler")
    x = sc.parallelize(range(0,3)).keyBy(lambda x: x*x)
    y = sc.parallelize(zip(range(0,5), range(0,5)))
    map((lambda (x,y): (x, (list(y[0]), (list(y[1]))))), sorted(x.cogroup(y).collect()))
    
    print "Use of KeyBy"
    z = sc.parallelize(zip(range(0,5), range(0,5), range(0,5), range(6,10), range(11,15)))
    keyBy = z.keyBy(lambda (x,y,z,e,d): (x,y,z))
    #[((0, 0, 0), (0, 0, 0, 6, 11))]
    print keyBy.take(1)
