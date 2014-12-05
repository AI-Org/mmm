-- Function: wj2level.gibbs_summary(text, text, text, bigint, bigint)

-- DROP FUNCTION wj2level.gibbs_summary(text, text, text, bigint, bigint);

CREATE OR REPLACE FUNCTION wj2level.gibbs_summary(model_name text, hierarchy_level1 text, hierarchy_level2 text, raw_iters bigint, burn_in bigint)
  RETURNS text AS
$BODY$ 

declare 

summary_geweke_conv_diag_detailed_sql text;
summary_geweke_conv_diag_sql text;
text_output text; 

begin 


-- Compute Geweke Convergence Diagnostic (CD) to confirm that the draws of beta_i from the Gibbs Sampler are stationary.  
-- Break up post burn-in draws from the Gibbs Sampler into 3 pieces.  
-- 1st piece: First 10% of the draws.
-- 3rd piece: Last 40% of the draws
-- 2nd piece: "Middle" draws that are not in the 1st or 3rd piece
-- The CD is computed by making comparisons between the 1st and 3rd pieces.  If characteristics of the 1st and 3rd piece are similar, then there isn't significant evidence that the Sampler has not converged.  See Koop pp.66 for more details.

-- Compute CD and store in a table.  CD is assumed to follow a Standard Normal Distribution.
execute 'drop table if exists '||model_name||'_summary_geweke_conv_diag_detailed';

summary_geweke_conv_diag_detailed_sql = 'create table '||model_name||'_summary_geweke_conv_diag_detailed as select 
*
, (avg_sa_i-avg_sc_i)/(se_sa_i+se_sc_i) as cd_beta_i 
from 
(select '||hierarchy_level2||'
, '||hierarchy_level1||'
, driver
, stddev_samp(beta_i_draw)/(sqrt(.1*('||raw_iters||'-'||burn_in||'))) as se_sa_i
, avg(beta_i_draw) as avg_sa_i 
from '||model_name||'_beta_i_draw_long 
where iter<(.1*('||raw_iters||'-'||burn_in||')) 
group by 1,2,3) a 
join 
(select '||hierarchy_level2||'
, '||hierarchy_level1||'
, driver
, stddev_samp(beta_i_draw)/(sqrt(('||raw_iters||'-'||burn_in||')-.6*('||raw_iters||'-'||burn_in||'))) as se_sc_i
, avg(beta_i_draw) as avg_sc_i 
from '||model_name||'_beta_i_draw_long 
where iter>(.6*('||raw_iters||'-'||burn_in||')) 
group by 1,2,3) c 
using ('||hierarchy_level2||', '||hierarchy_level1||', driver)
distributed by ('||hierarchy_level2||', '||hierarchy_level1||')
';

execute summary_geweke_conv_diag_detailed_sql;







-- Count number of coefficients where the CD falls outside of the 95% interval.  By chance alone, 5% of the marginal posterior distributions should appear non-stationary when stationarity exists (http://www.bayesian-inference.com/softwaredoc/Geweke.Diagnostic).  
execute 'drop table if exists '||model_name||'_summary_geweke_conv_diag';

summary_geweke_conv_diag_sql = 'create table '||model_name||'_summary_geweke_conv_diag 
as select 
cd_signif::float/denom::float as cd_pct 
from 
(select count(*) denom from '||model_name||'_summary_geweke_conv_diag_detailed) a
, 
(select count(*) cd_signif from '||model_name||'_summary_geweke_conv_diag_detailed where abs(cd_beta_i)>1.96) b 
distributed randomly';

execute summary_geweke_conv_diag_sql;

/*-- 42 out of 1890 --> Conclude that there is not sufficient evidence of non-stationarity.  In other words, there isn't evidence that the Gibbs Sampler has not converged.
*/

text_output := 'Done: Gibbs Sampler draws have been summarized.  All objects associated with this model are named with a '''||model_name||''' prefix.';
return text_output;

end; 
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION wj2level.gibbs_summary(text, text, text, bigint, bigint)
  OWNER TO gpadmin;
