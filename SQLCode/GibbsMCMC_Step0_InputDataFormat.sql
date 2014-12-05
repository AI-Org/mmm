/*
--------------------------------------------------------------------------------
-- Gibbs Sampler Step 1: Prepare data and set initial values
-- Reference:  Gary Koop, Bayesian Econometrics, Wiley 2003.  http://www.wiley.com/legacy/wileychi/koopbayesian/
--------------------------------------------------------------------------------
*/

-- Set schema path
set search_path to wj2level;


-- Grab columns of data from relevant tables and store in a single temporary table
drop table if exists d_temp;
create table d_temp as 
select * from 
(
(
anfnew.historic_data_agg_breakout_transformed_final 
join 
(select item_brand_code, brand_department_number from anfnew.big_table_nov2011_jan2014 group by 1,2) c 
using (brand_department_number) 
) a  
join 
(select brand_department_number, tier, week, num_colors_log as num_colors_log_new, num_colors_in_log, num_colors_alt_log, item_promo_pct as item_promo_pct_new, redline_pct, store_promo_pct as store_promo_pct_new, promo_disc_regular_avg_log, redline_discount_total_avg_log, holiday_pre_xmas from agg.historic_data_agg_transformed ) d 
using (brand_department_number, tier, week) 
)
p 
join 
agg.new_drivers_transformed  
using(brand_department_name, tier, week) 
distributed by (brand_department_number, tier) 
;
-- SELECT 15969
-- Time: 10211.470 ms


-- Transform data & grab a selection of columns from d_temp and store in a new table named d
drop table if exists d;
create table d as select 
* 
, ln(greatest(baseprice_minus_promo_regular_discount,0)+1) as baseprice_minus_promo_regular_discount_log
from 
(
select 
item_brand_code
, gender_code
, brand_department_number
, brand_department_name
, tier
, item_brand_name
, week
, blended_units_total_log
, coalesce(num_colors_log_new,0) as num_colors_log_new   
, coalesce(num_colors_in_log,0) as num_colors_in_log    
, coalesce(sellable_qty_log,0) as  sellable_qty_log       
, coalesce(floorset_bts_ind,0) as  floorset_bts_ind      
, coalesce(holiday_thanksgiving,0) as holiday_thanksgiving   
, coalesce(holiday_xmas,0) as  holiday_xmas          
, coalesce(holiday_pre_xmas,0) as  holiday_pre_xmas   
, coalesce(price_non_promo_regular_smooth_log,0) as  price_non_promo_regular_smooth_log
, (coalesce(promo_disc_regular_avg_log,0) + coalesce(redline_discount_total_avg_log,0)) as discount_log  
, coalesce(promo_disc_regular_avg_log,0) as discount_promo_regular_log  
, coalesce(redline_discount_total_avg_log,0) as discount_redline_total_log   
, ((exp(coalesce(price_non_promo_regular_smooth_log,0))-1) - (exp(coalesce(promo_disc_regular_avg_log,0))-1)) as baseprice_minus_promo_regular_discount   
, coalesce(wt_promo_units_regular,0) as wt_promo_units_regular
, coalesce(wt_non_promo_units_redline,0) as wt_non_promo_units_redline
, coalesce(wt_promo_units_redline,0) as  wt_promo_units_redline 
, coalesce(holiday_easter,0) as holiday_easter
, coalesce(item_promo_regular_pct,0) as item_promo_regular_pct
, coalesce(item_promo_redline_pct,0) as item_promo_redline_pct
, coalesce(item_promo_pct,0) as item_promo_pct
, coalesce(store_promo_pct,0) as store_promo_pct
, coalesce(item_promo_pct_new,0) as item_promo_pct_new
, coalesce(store_promo_pct_new,0) as store_promo_pct_new 
, coalesce(redline_pct,0) as redline_pct 
, coalesce(total_sellable_qty_log,0) as total_sellable_qty_log 
, coalesce(redline_item_promo_pct,0) as redline_item_promo_pct 
from 
d_temp 
join 
(select 
item_brand_code 
,gender_code 
,brand_department_number
,brand_department_name 
,tier, count(*) from d_temp group by 1,2,3,4,5 having count(*)>100) as foo 
using (item_brand_code 
,gender_code 
,brand_department_number
,brand_department_name 
,tier)
) as d1 
; 
--15820


-- Restore table named d (only done when migrating) 
drop table if exists d;
create table d as select * from public.d;








