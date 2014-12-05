/*
--------------------------------------------------------------------------------
-- Gibbs Sampler Step 3: After getting the 1st draw of the Gibbs Sampler from Step 2 via the gibbs_init() function, we use the "meta-UDF" below, gibbs(), to get all subsequent draws from the Gibbs Sampler.  In the case of the delivered Demand Models, we went up to about 12,000 draws just because we could.  However, we have seen the Gibbs Sampler stabilize and become stationary even after about 4000 draws.    
-- Reference:  Gary Koop, Bayesian Econometrics, Wiley 2003.  http://www.wiley.com/legacy/wileychi/koopbayesian/
--------------------------------------------------------------------------------  
*/

-- The "meta-UDF" below, gibbs(), provides an interface to get the first draw of the Gibbs Sampler for end users.  This UDF, along with the other "meta-UDF" named gibbs_init() as defined in the Step 2 script, are really the only 2 UDFs that should be readily exposed to end users who would regularly build these Demand Models.  The comments below describe each of the arguments of the gibbs() function.


-- 'source_table' = Name of source table, arranged in a form where each variable is represented as a separate column.
-- 'hierarchy_level1' = Name of the bottom-most level of the hierarchy.
-- 'hierarchy_level2' = Name of the upper-most level of the hierarchy.  The "pooling level" of the hierarchy.
-- 'p' = Number of explanatory variables in the model, including the intercept term.
-- 'df1'  = Degrees of freedom for Wishart distribution, used when sampling the Level2 covariance matrix.  A reasonable off-the-shelf value is to set 'df1' to p+1.
-- 'y_var' = Name of the response variable.
-- 'x_var_array' = Names of the explanatory variables provided within an array expression.
-- 'coef_means_prior_array' = Priors for coefficient means at the upper-most level of the hierarchy.  See next line on how to make the prior negligible (i.e. how to specify a noninformative prior under this framework).
-- 'coef_precision_prior_array' = Priors for coefficient covariances at the upper-most level of the hierarchy.  If no prior information would want to be incorporated, the user would specify arbitrary values for 'coef_means_prior_array' and specify a set of very small numbers for 'coef_precision_prior_array', such as .000001.
-- 'sample_size_deflator' = Decreases the confidence that the user has about the data, applicable in situations where the data is known to be not perfect.  Effectively decreases the precision of the data on the final estimated coefficients.  For example, specifying a value of 10 for 'sample_size_deflator' would very roughly decrease the size and precision of the sample perceived by the Gibbs Sampler by a factor of 10.  
-- 'begin_iter' = The iteration number from which the gibbs() UDF should start.  In most cases, this value would be set to 2, since gibbs() would typically be run right after the first draw of the Gibbs Sampler provided by the gibbs_init() UDF.  However, in some cases, if the user wants to pick up where she left off and draw additional iterations from a Gibbs Sampler that she had started previously (and if she had drawn 5000 iterations in that previous workflow), she would set 'begin_iter' equal to 5000+1=5001.
-- 'end_iter' = The number of desired iterations from the Gibbs Sampler

create or replace function gibbs(model_name text
, source_table text
, hierarchy_level1 text
, hierarchy_level2 text
, p bigint
, df1 bigint
, y_var text
, x_var_array text
, coef_means_prior_array text
, coef_precision_prior_array text
, sample_size_deflator float8
, begin_iter bigint
, end_iter bigint) returns text as 
$$ 

declare 
Vbeta_j_mu_sql text;
Vbeta_inv_Sigmabeta_j_draw_sql text;
beta_mu_j_sql text;
beta_mu_j_draw_sql text;
Vbeta_i_sql text;
beta_i_mean_sql text;
beta_i_draw_sql text;
s2_sql text;
h_draw_sql text;

beta_i_draw_long_sql text;

text_output text;

begin 
for s in begin_iter..end_iter loop 


-- Compute Vbeta_i
Vbeta_i_sql = 'insert into '||model_name||'_Vbeta_i 
select 
'||s||'
, '||hierarchy_level2||' 
, '||hierarchy_level1||' 
, pinv(matrix_add_plr((matrix_scalarmult_plr(xtx, h_draw)), Vbeta_inv_j_draw)) as Vbeta_i 
from 
( 
'||model_name||'_d_array_agg_constants a 
join 
(select iter, '||hierarchy_level2||', Vbeta_inv_j_draw from '||model_name||'_Vbeta_inv_Sigmabeta_j_draw where iter='||s||'-1) foo  
using ('||hierarchy_level2||') 
) foo3 
join 
(select '||hierarchy_level2||', h_draw from '||model_name||'_h_draw where iter='||s||'-1) foo2 
using ('||hierarchy_level2||') 
where iter='||s||'-1   
group by 1,2,3,4';

execute Vbeta_i_sql;

RAISE NOTICE 'Vbeta_i - s is %', s ;



-- Compute beta_i_mean 
beta_i_mean_sql = 'insert into '||model_name||'_beta_i_mean 
select 
'||s||'   
, '||hierarchy_level2||' 
, '||hierarchy_level1||' 
, beta_i_mean(Vbeta_i, h_draw, xty, Vbeta_inv_j_draw, beta_mu_j_draw) as beta_i_mean  
from 
(
(
select b.*
, a.xty 
from '||model_name||'_d_array_agg_constants a 
join '||model_name||'_Vbeta_i b 
using ('||hierarchy_level2||', '||hierarchy_level1||') where iter='||s||' 
) as foo 
join 
(
select '||hierarchy_level2||', Vbeta_inv_j_draw, beta_mu_j_draw  
from 
(select '||hierarchy_level2||', Vbeta_inv_j_draw from '||model_name||'_Vbeta_inv_Sigmabeta_j_draw where iter='||s||'-1) a   
join 
(select '||hierarchy_level2||', beta_mu_j_draw from '||model_name||'_beta_mu_j_draw where iter='||s||'-1) b  
using ('||hierarchy_level2||')
) as foo2 
using ('||hierarchy_level2||') 
) as foo4
join 
(select '||hierarchy_level2||', h_draw from '||model_name||'_h_draw where iter='||s||'-1) foo3 
using ('||hierarchy_level2||')
'; 

execute beta_i_mean_sql;

RAISE NOTICE 'beta_i_mean- s is %', s ;


-- Draw new beta_i using iter=s-1 values.  Draw from mvnorm dist'n.
beta_i_draw_sql = 'insert into '||model_name||'_beta_i_draw 
select 
'||s||'    
, '||hierarchy_level2||'
, '||hierarchy_level1||' 
, beta_draw(beta_i_mean, Vbeta_i) as beta_i_draw  
from 
(select 
a.*
, b.Vbeta_i 
from 
'||model_name||'_beta_i_mean a
join 
'||model_name||'_Vbeta_i b 
using ('||hierarchy_level2||', '||hierarchy_level1||',iter) 
where a.iter='||s||' and b.iter='||s||' 
) as foo
';

execute beta_i_draw_sql;

RAISE NOTICE 'beta_i_draw - s is %', s ;


-- Update the Vbeta_j_mu table with a new value (record), using the most "recent" values of the beta_i_draw coefficients (at iter=s, in the code just above this one).  The values of beta_mu_j_draw and Vbeta_k_mu have not yet been updated at this stage, so their values at iter=s-1 are taken. 

Vbeta_j_mu_sql = 'insert into '||model_name||'_Vbeta_j_mu 
select 
'||s||' 
, '||hierarchy_level2||' 
, matrix_add_diag_plr(sum(Vbeta_i_mu(beta_i_draw, beta_mu_j_draw)), '||p||') as Vbeta_j_mu  
from 
(select '||hierarchy_level2||', '||hierarchy_level1||', beta_i_draw from '||model_name||'_beta_i_draw where iter='||s||') a 
join 
(select '||hierarchy_level2||', beta_mu_j_draw from '||model_name||'_beta_mu_j_draw where iter='||s||'-1) b 
using ('||hierarchy_level2||')    
group by 1,2
'; 

execute Vbeta_j_mu_sql;

RAISE NOTICE 'Vbeta_j_mu - s is %', s ;


-- Draw Vbeta_inv and compute resulting sigmabeta using the above functions for each j.  The values of Vbeta_inv_j_draw is taken from iter=s, while the values of the other input parameters that have not yet been updated at iter=s, namely sigmabeta_inv_k, are taken from iter=s-1.    
Vbeta_inv_Sigmabeta_j_draw_sql = 'insert into '||model_name||'_Vbeta_inv_Sigmabeta_j_draw 
select 
'||s||'
, '||hierarchy_level2||'
, n1 
, Vbeta_inv_j_draw  
, pinv(matrix_add_plr(matrix_scalarmult_plr(Vbeta_inv_j_draw,n1),matrix_diag_plr('||coef_precision_prior_array||'))) as  Sigmabeta_j 
from 
(select '||hierarchy_level2||', Vbeta_inv_draw(('||df1||'), pinv(matrix_scalarmult_plr(Vbeta_j_mu, ('||df1||')))) as Vbeta_inv_j_draw from '||model_name||'_Vbeta_j_mu where iter='||s||') as foo 
join 
(select '||hierarchy_level2||', n1 from '||model_name||'_d_childcount group by 1,2) foo2 
using ('||hierarchy_level2||')
';

execute Vbeta_inv_Sigmabeta_j_draw_sql;

RAISE NOTICE 'Vbeta_inv_Sigmabeta_j_draw - s is %', s ;


-- Compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  Get back one coefficient vector for each j (i.e. J  coefficient vectors are returned).
beta_mu_j_sql = 'insert into '||model_name||'_beta_mu_j 
select 
'||s||'   
, '||hierarchy_level2||' 
, beta_mu_prior(Sigmabeta_j, Vbeta_inv_j_draw, sum_coef_j, '||coef_means_prior_array||', '||coef_precision_prior_array||' ) as beta_mu_j 
from 
(select * from '||model_name||'_Vbeta_inv_Sigmabeta_j_draw where iter='||s||') a 
join 
(select '||hierarchy_level2||', sum(beta_i_draw) as sum_coef_j from '||model_name||'_beta_i_draw where iter='||s||' group by 1) b
using ('||hierarchy_level2||')
'; 

execute beta_mu_j_sql;

RAISE NOTICE 'beta_mu_j - s is %', s ;


-- Draw beta_mu from mvnorm dist'n.  Get back J vectors of beta_mu, one for each J.  Note that all input values are at iter=s.
beta_mu_j_draw_sql = 'insert into '||model_name||'_beta_mu_j_draw  
select 
'||s||'  
, '||hierarchy_level2||' 
, beta_draw(beta_mu_j, Sigmabeta_j) as beta_mu_j_draw 
from 
(select '||hierarchy_level2||', beta_mu_j from '||model_name||'_beta_mu_j where iter='||s||') a 
join 
(select '||hierarchy_level2||', Sigmabeta_j from '||model_name||'_Vbeta_inv_Sigmabeta_j_draw where iter='||s||') b 
using ('||hierarchy_level2||')
'; 

execute beta_mu_j_draw_sql;

RAISE NOTICE 'beta_mu_j_draw - s is %', s ;



-- Compute updated value of s2 to use in next section.
s2_sql = 'insert into '||model_name||'_s2 
select '||s||' as iter 
, '||hierarchy_level2||' 
, count_grpby_level2 
, sum(ssr)/(count_grpby_level2 /'||sample_size_deflator||') as s2 
from 
(select '||hierarchy_level2||', '||hierarchy_level1||', iter
, sum((y-madlib.array_dot(beta_i_draw, x))^2) as ssr 
from 
(select 
beta_i_draw
, iter
, '||x_var_array||' as x
, '||y_var||' as y
, '||hierarchy_level2||'
, '||hierarchy_level1||' 
from '||model_name||'_beta_i_draw a 
join 
'||source_table||' b 
using ('||hierarchy_level2||','||hierarchy_level1||') 
where iter='||s||') as foo 
group by '||hierarchy_level2||','||hierarchy_level1||', iter) as foo2 
join 
(select '||hierarchy_level2||', count_grpby_level2  from '||model_name||'_d_count_grpby_level2 group by 1,2) as foo3 
using ('||hierarchy_level2||') 
group by iter, '||hierarchy_level2||', count_grpby_level2 ';

execute s2_sql;

RAISE NOTICE 's2 - s is %', s ;


-- Draw h from gamma dist'n.  Note that h=1/(s^2)
h_draw_sql = 'insert into '||model_name||'_h_draw 
select 
'||s||'
, '||hierarchy_level2||' 
, h_draw(1/(s2), count_grpby_level2 /'||sample_size_deflator||') 
from '||model_name||'_s2 
where iter='||s||'
group by 1,2,3';

execute h_draw_sql;

RAISE NOTICE 'h_draw - s is %', s ;

RAISE NOTICE 's is %', s ;

END LOOP;


-- Convert the array-based draws from the Gibbs Sampler into a "vertically long" format by unnesting the arrays.  
execute 'drop table if exists '||model_name||'_beta_i_draw_long';

beta_i_draw_long_sql = 'create table '||model_name||'_beta_i_draw_long as 
select *
, '||hierarchy_level2||'||''-:-''||'||hierarchy_level1||'||''-:-''||driver as '||hierarchy_level2||'_'||hierarchy_level1||'_driver 
from 
(select 
 iter
, '||hierarchy_level2||'
, '||hierarchy_level1||'
, unnest(beta_i_draw) beta_i_draw
, unnest(
string_to_array(
replace(
replace(
replace(
replace('''||x_var_array||''', '' '', '''')
, ''array['', '''')
,''
'', '''')
, '']'', '''')
, '','')
) as driver  
from '||model_name||'_beta_i_draw 
) as foo 
distributed by ('||hierarchy_level2||','||hierarchy_level1||')';

execute beta_i_draw_long_sql;

/* 
--Check that ordering of iterations is happening correctly 
select avg(beta_i_draw[9]) from beta_i_draw where brand_department_number=122 and tier='F1';
select avg(beta_i_draw) from beta_i_draw_long where  brand_department_number=122 and tier='F1' and driver='discount_redline_total_log';
select avg(beta_i_draw) from beta_i_draw_long2 where  brand_department_number=122 and tier='F1' and driver='discount_redline_total_log';
 
select avg(beta_i_draw[9]) from beta_i_draw where brand_department_number=122 and tier='F1' and iter=2931;
select avg(beta_i_draw) from beta_i_draw_long where  brand_department_number=122 and tier='F1' and driver='discount_redline_total_log' and iter=2931;
select avg(beta_i_draw) from beta_i_draw_long2 where  brand_department_number=122 and tier='F1' and driver='discount_redline_total_log' and iter=2931;

select beta_i_draw[9] from beta_i_draw where brand_department_number=122 and tier='F1' and iter>2931 order by iter limit 5;
select beta_i_draw from beta_i_draw_long where  brand_department_number=122 and tier='F1' and driver='discount_redline_total_log'  and iter>2931 order by iter limit 5;
select beta_i_draw from beta_i_draw_long2 where  brand_department_number=122 and tier='F1' and driver='discount_redline_total_log'  and iter>2931 order by iter limit 5; 
*/

text_output := 'Done: All requested Gibbs Sampler updates are complete.  All objects associated with this model are named with a '''||model_name||''' prefix.';
return text_output;

end; 
$$ 
language plpgsql;




