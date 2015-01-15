import sys
import re

from pyspark import SparkContext
import gibbs_init as gi
import gibbs

def parseData(data):
    columns = re.split(",", data)
    return columns 
    #return (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)

def load(source):
    return sc.textFile(source).map(lambda datapoint: parseData(datapoint))
    #return sc.textFile(source)

if __name__ == "__main__":
    """
        Usage: gibbs_execution.py [file]
        args:
            'file'          : hdfs:///data/d.csv { Name of source table, arranged in a form where each variable is represented as a separate column }
            'hierarchy_level1' : Name of the bottom-most level of the hierarchy.
            'hierarchy_level2' : Name of the upper-most level of the hierarchy.  The "pooling level" of the hierarchy.
            'p'           : Number of explanatory variables in the model, including the intercept term. Defaults to 14.
            'df1'         : Degrees of freedom for Wishart distribution, used when sampling the Level2 covariance matrix.  A reasonable off-the-shelf value is to set 'df1' to p+1, i.e. 15
            'y_var'       : Index of the response variable in the csv file. ( blended_units_total_log. )
            'x_var_array' : Indexes of the explanatory variables provided within an array expression. 
                          ,[1  
                          , num_colors_log_new         
                          , total_sellable_qty_log              
                          , floorset_bts_ind              
                          , holiday_thanksgiving    
                          , holiday_xmas            
                          , holiday_easter       
                          , baseprice_minus_promo_regular_discount_log 
                          , discount_redline_total_log  
                          , store_promo_pct_new  
                          , item_promo_pct_new 
                          , redline_pct 
                          , redline_item_promo_pct 
                          , holiday_pre_xmas]
            'coef_means_prior_array' : Priors for coefficient means at the upper-most level of the hierarchy.  
                          See next line on how to make the prior negligible (i.e. how to specify a noninformative prior under this framework).
            'coef_precision_prior_array' = Priors for coefficient covariances at the upper-most level of the hierarchy.  
                          If no prior information would want to be incorporated, the user would specify arbitrary values for 'coef_means_prior_array' and 
                          specify a set of very small numbers for 'coef_precision_prior_array', such as .000001.
            'sample_size_deflator' = Decreases the confidence that the user has about the data, applicable in situations where the data is known to be not perfect.  
                          Effectively decreases the precision of the data on the final estimated coefficients.  
                          For example, specifying a value of 10 for 'sample_size_deflator' would very roughly decrease the size and 
                          precision of the sample perceived by the Gibbs Sampler by a factor of 10.  
         run : spark-submit --py-files gibbs_init.py,gibbs_udfs.py,wishart.py,nearPD.py gibbs_execution.py
    """
    sc = SparkContext(appName="GibbsSampler")
    file = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///data/d.csv" 
    d = load(file) ## all data as separate columns
    keyBy_groupby_h2_h1 = d.keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).groupByKey().cache()     
    print keyBy_groupby_h2_h1.take(1)

    hierarchy_level1 = sys.argv[1] if len(sys.argv) > 1 else 1
    hierarchy_level2 = sys.argv[2] if len(sys.argv) > 2 else 2    
    #'p' = Number of explanatory variables in the model, including the intercept term.
    p = sys.argv[3] if len(sys.argv) > 3 else 14  # todo convert sysarhs to int
    # df1 = defree of freedom 
    df1 = sys.argv[4] if len(sys.argv) > 4 else 15    
    
    y_var_index = 4
    x_var_indexes = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
    
    # coef_means_prior_array' = Priors for coefficient means at the upper-most level of the hierarchy. 
    # coef_means_prior_array=sys.argv[5] if len(sys.argv) > 5 else [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    coef_means_prior_array = sys.argv[5] if len(sys.argv) > 5 else [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    
    # coef_precision_prior_array = Priors for coefficient covariances at the upper-most level of the hierarchy. 
    # coef_precision_prior_array=sys.argv[5] if len(sys.argv) > 5 else [1,1,1,1,1,1,1,1,1,1,1,1,1]
    coef_precision_prior_array = sys.argv[5] if len(sys.argv) > 5 else [1,1,1,1,1,1,1,1,1,1,1,1,1,1]
    
    sample_size_deflator = 1
    initial_vals = sys.argv[2] if len(sys.argv) > 2 else "ols" 

    # First initialize the gibbs sampler
    gi.gibbs_init_test(sc, d, keyBy_groupby_h2_h1, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals)
    
    # Calling the iterative gibbs algorithm 
    #gibbs.gibbs_test(sc, d, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, begin_iter, end_iter, keyBy_groupby_h2_h1)
    # print d_array_agg_sql
    sc.stop()
