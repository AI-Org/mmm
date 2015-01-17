import sys
import re

from pyspark import SparkContext
import gibbs_init as gi
import gibbs
import gibbs_summary as gis

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
            'hierarchy_level1' : Name of the bottom-most level of the hierarchy. ( tier )
            'hierarchy_level2' : Name of the upper-most level of the hierarchy.  The "pooling level" of the hierarchy. ( brand_department_number )
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
    ## load all data as separate columns
    d = load(file) 
    keyBy_groupby_h2_h1 = d.keyBy(lambda (index, hierarchy_level1, hierarchy_level2, week, y1, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13): (hierarchy_level2, hierarchy_level1)).groupByKey().cache()     
    print "Cached Copy of Data, First Data Set : ", keyBy_groupby_h2_h1.take(1)
    
    # hierarchy_level1 = tier, 
    # hierarchy_level2 = brand_department_number
    hierarchy_level1 = sys.argv[2] if len(sys.argv) > 2 else 1
    hierarchy_level2 = sys.argv[3] if len(sys.argv) > 3 else 2
    
    # 'p' = Number of explanatory variables in the model, including the intercept term.
    p = sys.argv[4] if len(sys.argv) > 4 else 14  # todo convert sysarhs to int
    # df1 = defree of freedom 
    df1 = sys.argv[5] if len(sys.argv) > 5 else 15    
    # the response variable
    y_var_index = sys.argv[6] if len(sys.argv) > 6 else 4
    # the explanatory variables
    x_var_indexes = sys.argv[7] if len(sys.argv) > 7 else [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
    
    # coef_means_prior_array' = Priors for coefficient means at the upper-most level of the hierarchy. 
    # coef_means_prior_array=sys.argv[5] if len(sys.argv) > 5 else [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    coef_means_prior_array = sys.argv[8] if len(sys.argv) > 8 else [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    
    # coef_precision_prior_array = Priors for coefficient covariances at the upper-most level of the hierarchy. 
    # coef_precision_prior_array=sys.argv[5] if len(sys.argv) > 5 else [1,1,1,1,1,1,1,1,1,1,1,1,1]
    coef_precision_prior_array = sys.argv[9] if len(sys.argv) > 9 else [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
    
    sample_size_deflator = sys.argv[10] if len(sys.argv) > 10 else 1
    
    initial_vals = sys.argv[11] if len(sys.argv) > 11 else "ols" 

    # First initialize the gibbs sampler
    (m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw , m1_ols_beta_i ,m1_ols_beta_j ,m1_s2 ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_j_mu) = gi.gibbs_initializer(sc, d, keyBy_groupby_h2_h1, hierarchy_level1, hierarchy_level2, p, df1, y_var_index, x_var_indexes, coef_means_prior_array, coef_precision_prior_array, sample_size_deflator, initial_vals)
    
    begin_iter = sys.argv[12] if len(sys.argv) > 12 else 2
    end_iter = sys.argv[13] if len(sys.argv) > 13 else 4    
    
    # Calling the iterative gibbs algorithm 
    (m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw  ,m1_s2 ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_j_mu, m1_beta_i_draw_long_keyBy_h2_h1_driver) = gibbs.gibbs_iter(sc, begin_iter, end_iter, m1_beta_i_draw ,m1_beta_i_mean ,m1_beta_mu_j ,m1_beta_mu_j_draw ,m1_d_array_agg ,m1_d_array_agg_constants ,m1_d_childcount,m1_d_count_grpby_level2 ,m1_h_draw ,m1_s2 ,m1_Vbeta_i ,m1_Vbeta_inv_Sigmabeta_j_draw ,m1_Vbeta_j_mu)
    
    raw_iters = sys.argv[14] if len(sys.argv) > 14 else 100
    
    burn_in = sys.argv[15] if len(sys.argv) > 15 else 0     
    
    # call gibbs summary functions
    m1_summary_geweke_conv_diag_detailed = gis.m1_summary_geweke_conv_diag_detailed(hierarchy_level1, hierarchy_level2, raw_iters, burn_in, m1_beta_i_draw_long_keyBy_h2_h1_driver)
    print "m1_summary_geweke_conv_diag_detailed count", m1_summary_geweke_conv_diag_detailed.count()
    print "m1_summary_geweke_conv_diag_detailed take 1", m1_summary_geweke_conv_diag_detailed.take(1)
    
    sc.stop()
