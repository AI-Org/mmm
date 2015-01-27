# -*- coding: utf-8 -*-
#"""
#Created on Tue Jan 27 08:33:07 2015
#
#@author: ssoni
# """

library(MCMCpack)

args <- commandArgs(TRUE)
print(args)
print(as.numeric(args))
df = as.numeric(args[1])
dim1 = as.numeric(args[2])
dim2 = as.numeric(args[3])
ar = c(as.double(args[4]))
print(length(args))
for (i in 5:(length(args))) {
  ar = c(ar,as.double(args[i]))
}
print(ar)

scale = matrix(ar,dim1,dim2)
print(scale)
result = rwish(df,scale)
print(result) 