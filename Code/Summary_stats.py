# -*- coding: utf-8 -*-
"""
Created on Fri Feb 13 10:38:55 2015

@author: ssoni
"""
import pickle

if __name__ == "__main__":
    print "printing the summary statistics"
    
    path = '/home/ssoni/mmm_t/Code/result_diag/cd_pct.data'
    f=open(path, 'rb')  
    obj_dict = pickle.load(f) 
    print "cd_signif , denom, cd_pct values"
    print obj_dict