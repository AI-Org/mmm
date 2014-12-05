/*
--------------------------------------------------------------------------------
-- Gibbs Sampler: Execution
-- This is an example script to show how to use the "Meta-UDF" and UDFs defined in the other code files. The UDFs in these .sql files must be created in the system before the example code below can be used.  
--------------------------------------------------------------------------------  
*/

-- Set search path
set search_path to wj2level;

-- Get the first draw from the Gibbs Sampler.  Note that this drops all existing tables storing previous runs of the Gibbs Sampler for a given model_name.  Supply a new model_name to preserve older models.
select * from 
gibbs_init(
'm1'
,'d'
, 'tier'
, 'brand_department_number'
, 14
, 15
, 'blended_units_total_log'
, 'array[1  
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
, holiday_pre_xmas 
]'
--,array[0,0,0,0,0,0,0,-3,-3,0,0,0,0,0]
,'array[0,0,0,0,0,0,0,0,0,0,0,0,0,0]' 
--,array[1,1,1,1,1,1,1,400,400,1,1,1,1,1]
,'array[1,1,1,1,1,1,1,1,1,1,1,1,1,1]' 
,1
,'random'
) ;

-- Update Gibbs samples.  Select beginning & ending number of iterations.  Here we specify a -3 prior for the coefficient of two pricing-related explanatory variables.  
select * from 
gibbs(
'm1'
, 'd'
, 'tier'
, 'brand_department_number'
, 14
, 15
, 'blended_units_total_log'
, 'array[1  
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
, holiday_pre_xmas 
]'
--,array[0,0,0,0,0,0,0,-3,-3,0,0,0,0,0]
,'array[0,0,0,0,0,0,0,0,0,0,0,0,0,0]' 
--,array[1,1,1,1,1,1,1,400,400,1,1,1,1,1]
,'array[1,1,1,1,1,1,1,1,1,1,1,1,1,1]' 
,1
,2
,100) ;

-- Summarize Gibbs Samples
select * from 
gibbs_summary(
'm1' 
, 'tier'
, 'brand_department_number'
, 100
, 0 
);


select * from 
gibbs_init(
'm1'
,'d'
, 'hierarchy_level1'
, 'hierarchy_level2'
, 14
, 15
, 'y1'
, 'array[1, x1 , x2 , x3 , x4 , x5 , x6, x7, x8, x9, x10 , x11 , x12 , x13 ]'
--,array[0,0,0,0,0,0,0,-3,-3,0,0,0,0,0]
,'array[0,0,0,0,0,0,0,0,0,0,0,0,0,0]' 
--,array[1,1,1,1,1,1,1,400,400,1,1,1,1,1]
,'array[1,1,1,1,1,1,1,1,1,1,1,1,1,1]' 
,1
,'random'
) ;

select * from 
gibbs(
'm1'
, 'd'
, 'hierarchy_level1'
, 'hierarchy_level2'
, 14
, 15
, 'y1'
, 'array[1, x1 , x2 , x3 , x4 , x5 , x6, x7, x8, x9, x10 , x11 , x12 , x13 ]'
--,'
--,array[0,0,0,0,0,0,0,-3,-3,0,0,0,0,0]
,'array[0,0,0,0,0,0,0,0,0,0,0,0,0,0]' 
--,array[1,1,1,1,1,1,1,400,400,1,1,1,1,1]
,'array[1,1,1,1,1,1,1,1,1,1,1,1,1,1]' 
,1
,2
,100) ;


select * from 
gibbs_summary(
'm1' 
, 'tier'
, 'brand_department_number'
, 100
, 0 
);
