"""
@author: ssoni

Gibbs summary
Call as  gibbs_summary( text, text, bigint, bigint)
"""
import gibbs_transformations as gtr
def m1_summary_geweke_conv_diag_detailed(hierarchy_level1, hierarchy_level2, raw_iters, burn_in, m1_beta_i_draw_long_keyBy_h2_h1_driver):
    """
        -- Compute Geweke Convergence Diagnostic (CD) to confirm that the draws of beta_i from the Gibbs Sampler are stationary.  
        -- Break up post burn-in draws from the Gibbs Sampler into 3 pieces.  
        -- 1st piece: First 10% of the draws.
        -- 3rd piece: Last 40% of the draws
        -- 2nd piece: "Middle" draws that are not in the 1st or 3rd piece
        -- The CD is computed by making comparisons between the 1st and 3rd pieces.  If characteristics of the 1st and 3rd piece are similar, then there isn't significant evidence that the Sampler has not converged.  See Koop pp.66 for more details.

        -- Compute CD and store in a table.  CD is assumed to follow a Standard Normal Distribution.
    """
    # structured as (h2, h1, driver) -> (s, h2, h1, beta_draw[i], x_array[i], h2_h1_driver) 
    m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_percent = m1_beta_i_draw_long_keyBy_h2_h1_driver.filter(lambda (x, y):(y[0]< 0.1 *(raw_iters - burn_in)))
    m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_percent = m1_beta_i_draw_long_keyBy_h2_h1_driver.filter(lambda (x, y):(y[0]> 0.6 * (raw_iters - burn_in)))
    
    m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10 = m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10_percent.groupByKey()
    geweke_part_10_percent = m1_beta_i_draw_long_keyBy_h2_h1_driver_first_10.map(lambda (x, y): (x, str.compute_sa_i_avg_sa(y, raw_iters, burn_in))).keyBy(lambda (x, y): x)
    
    m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40 = m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40_percent.groupByKey()
    geweke_part_40_percent = m1_beta_i_draw_long_keyBy_h2_h1_driver_last_40.map(lambda (x, y): (x, str.compute_se_sc_i_avg_sc_i(y, raw_iters, burn_in))).keyBy(lambda (x, y): x)
    
    Joined_geweke_part_10_percent_with_geweke_part_40_percent = geweke_part_10_percent.cogroup(geweke_part_40_percent)
    m1_summary_geweke_conv_diag_detailed = Joined_geweke_part_10_percent_with_geweke_part_40_percent.map(lambda (x,y): (x, list(y[0])[0][0], list(y[0])[0][1], list(y[1])[0][0], list(y[1])[0][1], str.get_cd_beta_i(list(y[0])[0][0], list(y[0])[0][1], list(y[1])[0][0], list(y[1])[0][1])))
    
    print "m1_summary_geweke_conv_diag_detailed count", m1_summary_geweke_conv_diag_detailed.count()
    print "m1_summary_geweke_conv_diag_detailed take 1", m1_summary_geweke_conv_diag_detailed.take(1)
    
    
    