/*
--------------------------------------------------------------------------------
-- Gibbs Sampler Step 2: The Gibbs Sampler is an iterative Markov Chain Monte Carlo (MCMC) algorithm so it needs to be initialized.  Using the initial values computed in Step 1, we initialize the Gibbs Sampler in the "meta-UDF" below, and end up with draws from the 1st MCMC iteration.  
-- Reference:  Gary Koop, Bayesian Econometrics, Wiley 2003.  http://www.wiley.com/legacy/wileychi/koopbayesian/
--------------------------------------------------------------------------------  
*/

-- The "meta-UDF" below, gibbs_init(), provides an interface to get the first draw of the Gibbs Sampler for end users.  This UDF, along with the other "meta-UDF" named gibbs() as defined in the Step 3 script, are really the only 2 UDFs that should be readily exposed to end users who would regularly build these Demand Models.  The comments below describe each of the arguments of the gibbs_init() function.

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

set search_path to wj2level; -- Set schema path

create or replace function gibbs_init(model_name text
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
, initial_vals text) returns text as 
$$ 

declare

d_array_agg_sql text;
d_array_agg_constants_sql text;
d_childcount_sql text;
d_count_grpby_level2_sql text;
Vbeta_j_mu_sql text;
Vbeta_inv_Sigmabeta_j_draw_sql text;
beta_mu_j_sql text;
beta_mu_j_draw_sql text;
Vbeta_i_sql text;
beta_i_mean_sql text;
beta_i_draw_sql text;
s2_sql text;
h_draw_sql text;
ols_beta_i_sql text;
ols_beta_j_sql text;
initialvals_beta_i_sql text;
initialvals_beta_j_sql text;
driver_list_sql text;

text_output text; 

begin 


-- We now pre-compute quantities that REMAIN CONSTANT throughout sampler so that they don't need to be computed needlessly at every iteration of the Gibbs Sampler.  


-- Create array aggregated version of d.  For the response variable, we collapse data from all weeks into a single array named y for each department_name-tier combo.  For the set of explanatory variables, in our case 14 explanatory variables, we collapse data from all weeks into a single array named x_matrix.  The data type of x_matrix is a 2-dimensional array.  There is one cell of x_matrix for each department_name-tier combo, and the dimension for each x_matrix is (# of weeks of data in the department_name-tier)x(# of explanatory variables), which equals (# of weeks of data in the department_name-tier)x(14) since we have 14 explanatory variables.  We end up with a table d_array_agg with as many rows as the number of distinct department_name-tier combos.  In our case we end up with 135 records.  

execute 'drop table if exists '||model_name||'_d_array_agg'; 

d_array_agg_sql = '
create table '||model_name||'_d_array_agg as select 
 '||hierarchy_level2||'  
,'||hierarchy_level1||'  
, madlib.matrix_agg('||x_var_array||') as x_matrix 
, madlib.matrix_agg(array['||y_var||']) as y 
from '||source_table||'  
group by '||hierarchy_level2||','||hierarchy_level1||'   
distributed by ('||hierarchy_level2||','||hierarchy_level1||');
'; 

execute d_array_agg_sql;    

-- At various steps the Gibbs Sampler, we need to make use of X'X and X'y.  We pre-compute these two quantities for each department_name-tier and name them xtx and xty, respectively.
execute 'drop table if exists '||model_name||'_d_array_agg_constants';

d_array_agg_constants_sql = 'create table '||model_name||'_d_array_agg_constants as 
select  
'||hierarchy_level2||','||hierarchy_level1||'  
, madlib.matrix_mem_mult(madlib.matrix_mem_trans(x_matrix),x_matrix) as xtx 
, madlib.matrix_mem_mult(madlib.matrix_mem_trans(x_matrix),y) as xty 
from '||model_name||'_d_array_agg 
distributed by ('||hierarchy_level2||','||hierarchy_level1||')';

execute d_array_agg_constants_sql;


-- The number of "children" for each level of the hierarchy also needs to be known at various steps of the Gibbs Sampler.  We compute the number of children at the brand level (# of children is equal to the number of genders per brand), the brand-gender level (# of children is equal to the number of departments per brand-gender), and at the department_name level (# of children is equal to the number tiers within each department_name).
execute 'drop table if exists '||model_name||'_d_childcount';

d_childcount_sql = 'create table '||model_name||'_d_childcount as 
select 
 '||hierarchy_level2||'
, n1  
from 
(
select 
 '||hierarchy_level2||' 
, count(distinct '||hierarchy_level1||') over (partition by '||hierarchy_level2||') as n1  
, '||hierarchy_level1||' 
from '||source_table||' 
group by 
'||hierarchy_level2||','||hierarchy_level1||'  
) foo 
group by 1,2 
distributed by ('||hierarchy_level2||')
';

execute d_childcount_sql;


-- Not all department_name-tiers have the same number of weeks of available data (i.e. the number of data points for each department_name-tier is not the same for all department_name-tiers).  We pre-compute this quantity for each department_name-tier.  
execute 'drop table if exists '||model_name||'_d_count_grpby_level2';

d_count_grpby_level2_sql = 'create table '||model_name||'_d_count_grpby_level2 as 
select 
'||hierarchy_level2||'
, count(*)::float8 as count_grpby_level2  
from '||source_table||' 
group by 1 
distributed by ('||hierarchy_level2||')
';

execute d_count_grpby_level2_sql;


-- Compute OLS estimates for reference 
execute 'drop table if exists '||model_name||'_ols_beta_i';
execute 'drop table if exists '||model_name||'_ols_beta_i_summary';
ols_beta_i_sql = 'select madlib.linregr_train('''||source_table||''','''||model_name||'_ols_beta_i'','''||y_var||''','''||x_var_array||''','''||hierarchy_level2||','||hierarchy_level1||''')';

execute ols_beta_i_sql; 

execute 'drop table if exists '||model_name||'_ols_beta_j'; 
execute 'drop table if exists '||model_name||'_ols_beta_j_summary'; 
ols_beta_j_sql = 'select madlib.linregr_train('''||source_table||''', '''||model_name||'_ols_beta_j'','''||y_var||''', '''||x_var_array||''', '''||hierarchy_level2||''')';

execute ols_beta_j_sql;




-- Set required initial values for Gibbs Sampler 
if initial_vals = 'ols' 

then 

-- Initial values of coefficients for each department_name-tier (i).  Set crudely as OLS regression coefficients for each department_name-tier (i).  
execute 'drop table if exists '||model_name||'_initialvals_beta_i';
execute 'create table '||model_name||'_initialvals_beta_i as select * from '||model_name||'_ols_beta_i';

--Initial values of coefficients for each department_name (j).  Set crudely as OLS regression coefficients for each department_name (j).  
execute 'drop table if exists '||model_name||'_initialvals_beta_j'; 
execute 'create table '||model_name||'_initialvals_beta_j as select * from '||model_name||'_ols_beta_j';

elseif initial_vals = 'random' 

then

-- Initial values of coefficients for each department_name-tier (i).  Set crudely as OLS regression coefficients for each department_name-tier (i).  
execute 'drop table if exists '||model_name||'_initialvals_beta_i';
initialvals_beta_i_sql = 'create table '||model_name||'_initialvals_beta_i as 
select 
'||hierarchy_level2||'
, '||hierarchy_level1||'
, initial_vals_random('||p||') as coef 
from 
(select '||hierarchy_level2||', '||hierarchy_level1||' from '||model_name||'_d_array_agg group by 1,2) a 
distributed by ('||hierarchy_level2||')';

execute initialvals_beta_i_sql; 
 
--Initial values of coefficients for each department_name (j).  Set crudely as OLS regression coefficients for each department_name (j).  
execute 'drop table if exists '||model_name||'_initialvals_beta_j'; 
initialvals_beta_j_sql = 'create table '||model_name||'_initialvals_beta_j as 
select 
'||hierarchy_level2||'
, initial_vals_random('||p||') as coef  
from 
(select '||hierarchy_level2||' from '||model_name||'_d_array_agg group by 1) a 
distributed by ('||hierarchy_level2||')';

execute initialvals_beta_j_sql;

end if;


-- Using the above initial values of the coefficients and drawn values of priors, compute initial value of coefficient var-cov matrix (Vbeta_i_mu) FOR EACH group i, with group j coefficients as priors, and then sum then to get back J matrices
execute 'drop table if exists '||model_name||'_Vbeta_j_mu';

Vbeta_j_mu_sql = 'create table '||model_name||'_Vbeta_j_mu as 
select 
1 as iter 
, '||hierarchy_level2||'  
, matrix_add_diag_plr(sum(Vbeta_i_mu(coef_i, coef_j)), '||p||') as Vbeta_j_mu  
from 
(
(select '||hierarchy_level2||', '||hierarchy_level1||', coef as coef_i from '||model_name||'_initialvals_beta_i) a   
join 
(select '||hierarchy_level2||', coef as coef_j from '||model_name||'_initialvals_beta_j) b 
using ('||hierarchy_level2||') 
) foo1 
group by 1,2 
distributed by ('||hierarchy_level2||')';  

execute Vbeta_j_mu_sql;



-- Draw Vbeta_inv and compute resulting sigmabeta using the above functions for each j 
execute 'drop table if exists '||model_name||'_Vbeta_inv_Sigmabeta_j_draw';

Vbeta_inv_Sigmabeta_j_draw_sql = 'create table '||model_name||'_Vbeta_inv_Sigmabeta_j_draw as 
select 
iter 
, '||hierarchy_level2||'
, n1 
, Vbeta_inv_j_draw  
, pinv(matrix_add_plr(matrix_scalarmult_plr(Vbeta_inv_j_draw,n1),matrix_diag_plr('||coef_precision_prior_array||'))) as  Sigmabeta_j 
from 
(select iter, '||hierarchy_level2||', Vbeta_inv_draw(('||df1||'), pinv(matrix_scalarmult_plr(Vbeta_j_mu, ('||df1||')))) as Vbeta_inv_j_draw from '||model_name||'_Vbeta_j_mu) as foo  
join 
(select '||hierarchy_level2||', n1 from '||model_name||'_d_childcount group by 1,2) foo2 
using ('||hierarchy_level2||')  
distributed by ('||hierarchy_level2||')';

execute Vbeta_inv_Sigmabeta_j_draw_sql;



-- Compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  Get back one coefficient vector for each j (i.e. J  coefficient vectors are returned).
execute 'drop table if exists '||model_name||'_beta_mu_j';

beta_mu_j_sql = 'create table '||model_name||'_beta_mu_j as 
select 
1 as iter 
, '||hierarchy_level2||' 
, beta_mu_prior(Sigmabeta_j, Vbeta_inv_j_draw, sum_coef_j, '||coef_means_prior_array||', '||coef_precision_prior_array||' ) as beta_mu_j 
from 
'||model_name||'_Vbeta_inv_Sigmabeta_j_draw a 
join  
(select '||hierarchy_level2||', sum(coef) as sum_coef_j from '||model_name||'_initialvals_beta_i group by 1) b 
using ('||hierarchy_level2||') 
distributed by ('||hierarchy_level2||')'; 

execute beta_mu_j_sql;


-- Draw beta_mu from mvnorm dist'n.  Get back J vectors of beta_mu, one for each J.
execute 'drop table if exists '||model_name||'_beta_mu_j_draw';

beta_mu_j_draw_sql = 'create table '||model_name||'_beta_mu_j_draw as 
select 
1 as iter
, '||hierarchy_level2||' 
, beta_draw(beta_mu_j, Sigmabeta_j) as beta_mu_j_draw 
from 
'||model_name||'_beta_mu_j a 
join 
'||model_name||'_Vbeta_inv_Sigmabeta_j_draw b 
using (iter, '||hierarchy_level2||')  
distributed by ('||hierarchy_level2||')'; 

execute beta_mu_j_draw_sql;


-- Compute Vbeta_i
execute 'drop table if exists '||model_name||'_Vbeta_i';

Vbeta_i_sql = 'create table '||model_name||'_Vbeta_i as 
select 
iter
, '||hierarchy_level2||' 
, '||hierarchy_level1||' 
, pinv(matrix_add_plr((matrix_scalarmult_plr(xtx, 1)), Vbeta_inv_j_draw)) as Vbeta_i 
from '||model_name||'_d_array_agg_constants 
join 
(select iter, '||hierarchy_level2||', vbeta_inv_j_draw from '||model_name||'_Vbeta_inv_Sigmabeta_j_draw) foo2 
using ('||hierarchy_level2||') 
group by 1,2,3,4       
distributed by ('||hierarchy_level2||')';

execute Vbeta_i_sql;


-- Compute beta_i_mean
execute 'drop table if exists '||model_name||'_beta_i_mean'; 

beta_i_mean_sql = 'create table '||model_name||'_beta_i_mean as 
select 
c.iter 
, '||hierarchy_level2||'
, '||hierarchy_level1||' 
, beta_i_mean(Vbeta_i, 1, xty, Vbeta_inv_j_draw, beta_mu_j_draw) as beta_i_mean 
from (
(select 
b.* 
, a.xty 
from '||model_name||'_d_array_agg_constants a 
join 
'||model_name||'_Vbeta_i b 
using ('||hierarchy_level2||','||hierarchy_level1||') 
) as foo
join 
'||model_name||'_Vbeta_inv_Sigmabeta_j_draw b 
using ('||hierarchy_level2||') 
)
join 
'||model_name||'_beta_mu_j_draw c 
using ('||hierarchy_level2||') 
distributed by ('||hierarchy_level2||')';

execute beta_i_mean_sql;

-- Draw beta_i from mvnorm dist'n
execute 'drop table if exists '||model_name||'_beta_i_draw'; 

beta_i_draw_sql = 'create table '||model_name||'_beta_i_draw as 
select 
iter 
, '||hierarchy_level2||' 
, '||hierarchy_level1||'
, beta_draw(beta_i_mean, Vbeta_i) as beta_i_draw 
from (select a.*, b.Vbeta_i from '||model_name||'_beta_i_mean a join '||model_name||'_Vbeta_i b using ('||hierarchy_level2||','||hierarchy_level1||') ) as foo 
distributed by ('||hierarchy_level2||')';

execute beta_i_draw_sql;


-- Compute updated value of s2 to use in next section.  
execute 'drop table if exists '||model_name||'_s2';

s2_sql = 'create table '||model_name||'_s2 as 
select 
iter 
, '||hierarchy_level2||' 
, count_grpby_level2
, sum(ssr)/(count_grpby_level2/'||sample_size_deflator||') as s2 
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
join '||source_table||' b 
using ('||hierarchy_level2||','||hierarchy_level1||')) as foo 
group by '||hierarchy_level2||','||hierarchy_level1||', iter) as foo2 
join (select '||hierarchy_level2||', count_grpby_level2 from '||model_name||'_d_count_grpby_level2 group by 1,2) as foo3 
using ('||hierarchy_level2||') 
group by iter, '||hierarchy_level2||', count_grpby_level2 
distributed randomly';

execute s2_sql;


-- Draw h from gamma distn.  Note that h=1/(s^2)
execute 'drop table if exists '||model_name||'_h_draw'; 

h_draw_sql = 'create table '||model_name||'_h_draw as 
select iter
, '||hierarchy_level2||'
, h_draw(1/(s2), count_grpby_level2/'||sample_size_deflator||') from '||model_name||'_s2 
group by 1,2,3 
distributed randomly';

execute h_draw_sql;

text_output := 'Done: Gibbs Sampler for model '''||model_name||''' is initialized.  Proceed to run updates of the sampler by using the gibbs() function.  All objects associated with this model are named with a '''||model_name||''' prefix.';
return text_output;

end;
$$ 
language plpgsql;
