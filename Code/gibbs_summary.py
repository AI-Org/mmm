"""
@author: ssoni

Gibbs summary
Call as  gibbs_summary( text, text, bigint, bigint)
"""

def m1_summary_geweke_conv_diag_detailed(hierarchy_level1, hierarchy_level2,raw_iters, burn_in):
    """
        -- Compute Geweke Convergence Diagnostic (CD) to confirm that the draws of beta_i from the Gibbs Sampler are stationary.  
        -- Break up post burn-in draws from the Gibbs Sampler into 3 pieces.  
        -- 1st piece: First 10% of the draws.
        -- 3rd piece: Last 40% of the draws
        -- 2nd piece: "Middle" draws that are not in the 1st or 3rd piece
        -- The CD is computed by making comparisons between the 1st and 3rd pieces.  If characteristics of the 1st and 3rd piece are similar, then there isn't significant evidence that the Sampler has not converged.  See Koop pp.66 for more details.

        -- Compute CD and store in a table.  CD is assumed to follow a Standard Normal Distribution.
    """
    