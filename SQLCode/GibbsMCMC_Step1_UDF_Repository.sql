/*
--------------------------------------------------------------------------------
-- Gibbs Sampler Step 0: Repository or "Bank" of component UDFs that are used throughout various parts of the Gibbs Sampler.
-- Reference:  Gary Koop, Bayesian Econometrics, Wiley 2003.  http://www.wiley.com/legacy/wileychi/koopbayesian/
--------------------------------------------------------------------------------
*/

-- Set schema path
set search_path to wj2level;


-- Function and UDA to compute median
CREATE OR REPLACE FUNCTION final_median(anyarray) RETURNS float8 AS
$$ 
DECLARE
  cnt INTEGER;
BEGIN
  cnt := (SELECT count(*) FROM unnest($1) val WHERE val IS NOT NULL);
  RETURN (SELECT avg(tmp.val)::float8 
            FROM (SELECT val FROM unnest($1) val
                    WHERE val IS NOT NULL 
                    ORDER BY 1 
                    LIMIT 2 - MOD(cnt, 2) 
                    OFFSET CEIL(cnt/ 2.0) - 1
                  ) AS tmp
         );
END
$$ LANGUAGE plpgsql;
 
CREATE AGGREGATE median2(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=final_median,
  INITCOND='{}'
);


-- Function for matrix addition, which takes two matrices as input.  
create or replace function matrix_add_plr(float8[], float8[]) 
returns float8[] as 
$$ 
result<- arg1+arg2
return(result)
$$ 
language 'plr';


-- Function to diagonalize 1d array
create or replace function matrix_diag_plr(float8[]) 
returns float8[] as 
$$ 
result<- diag(arg1) 
return(result)
$$ 
language 'plr';


-- Function to add a diagonal matrix to a given matrix.  First argument is the given matrix, and the second argument is the dimension of the diagonal matrix.
create or replace function matrix_add_diag_plr(float8[], float8) 
returns float8[] as 
$$ 
result<- arg1+diag(arg2) 
return(result)
$$ 
language 'plr';


--Function to multiply a scalar to a matrix
create or replace function matrix_scalarmult_plr(float8[],float8) 
returns float8[] as 
$$ 
result<- arg2*arg1
return(result)
$$ 
language 'plr';

    
-- Function to compute var-cov matrix for the pooled model, as defined in Equation (7.27) of Koop pp.156-157 
create or replace function Vbeta_i_mu(beta_i float8[], beta_mu float8[]) 
returns float8[] as 
$$ 
beta_i_diff<- as.matrix(beta_i-beta_mu)
Vbeta_i_mu<- beta_i_diff%*%t(beta_i_diff)
return(Vbeta_i_mu)
$$ 
language 'plr';


--Function to draw Vbeta_inv from Wishart dist'n, as shown in Equation (7.25) of Koop pp.156-157
create or replace function Vbeta_inv_draw(float8, float8[]) 
returns float8[] as 
$$ 
library(MCMCpack)
return(rwish(arg1,arg2))
$$ 
language 'plr';


-- Function to compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  This function allows for user-specified priors on the coefficients.  For use at highest level of the hierarchy.  (7.27) of Koop pp.157.

create or replace function beta_mu_prior(float8[], float8[], float8[], float8[], float8[]) 
returns float8[] as 
$$ 
beta_mu<- arg1%*%(arg2%*%arg3+as.matrix(arg4*arg5))
return(beta_mu)
$$ 
language 'plr';


-- Function to compute mean pooled coefficient vector to use in drawing a new pooled coefficient vector.  Takes in values computed in higher levels of the hierarchy as priors -- thus is used for all levels of the hierarchy except the highest level.  The highest level of the hierarchy uses the beta_mu_prior() function instead of the beta_mu_general() function.  (7.27) of Koop pp.157.
-- 
-- create or replace function beta_mu_general(float8[], float8[], float8[], float8[], float8[]) 
-- returns float8[] as 
-- $$ 
-- beta_mu<- arg1%*%(arg2%*%arg3 + arg4%*%arg5)
-- return(beta_mu)
-- $$ 
-- language 'plr';


-- Function to draw new beta, as defined in (7.25) or (7.26) of Koop pp.156.  Takes mean vector of the multivariate normal distribution as its first parameter, and the variance matrix of the multivariate normal distribution as its second parameter.  Used in cases where beta_i and beta_mu are drawn.
create or replace function beta_draw(float8[], float8[]) 
returns float8[] as 
$$ 
library(MASS)
beta_mu_draw<- mvrnorm(1, arg1, arg2) 
return(beta_mu_draw)
$$ 
language 'plr';


-- Function to compute Vbeta_i, as defined in Equation (7.25) of Koop pp.156.   Only computed at lowest level of the hierarchy (i.e. the level that "mixes" directly with the data, namely X'X).
create or replace function Vbeta_i(float8,float8[],float8[]) 
returns float8[] as 
$$ 
Vbeta_i<- solve(arg1*arg2+arg3)
return(Vbeta_i)   
$$ 
language 'plr';


-- Function to compute beta_i_mean, as defined in (7.25) of Koop pp.156.  Only computed at lowest level of the hierarchy (i.e. the level that "mixes" directly with the data, namely X'y).
create or replace function beta_i_mean(float8[], float8, float8[], float8[], float8[]) 
returns float8[] as 
$$ 
beta_i_mean<- arg1%*%(arg2*arg3 + arg4%*%arg5)
return(beta_i_mean) 
$$ 
language 'plr';


--Function to draw h from gamma dist'n, as defined in (7.28) of Koop pp.157.  
create or replace function h_draw(m float8, v float8) 
returns float8 as 
$$ 
g.shape<- v/2
g.rate<- v/(2*m)
g.draw<- rgamma(1, shape=g.shape, rate=g.rate)
return(g.draw)
$$
language 'plr';


-- Function to draw random array sample of p elements from the uniform(-1,1) dist'n
create or replace function initial_vals_random(p bigint) 
returns float8[] as 
$$ 
return(runif(p,-1,1))
$$
language 'plr';



