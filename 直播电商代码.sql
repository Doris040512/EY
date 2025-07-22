# --------------------------------------------------------------------
# -- Table 1a: Category Order and Sales Performance
#-- Daily order and gmv
# -- > to include the ones provided in data input sheet
# --------------------------------------------------------------------

with raw as(
select distinct  country,
        date(cast(new_LS_start_time as datetime)) as grass_date,
        new_order_ls_session_id,
        order_ls_streamer_type,
        order_cal_type,
        new_order_ls_streamer_id,
        item_amount,
        new_order_id,
        new_gmv_usd,
        category_name,
        new_category_id,
        new_buyer_id,
        new_order_item_id,
        new_order_model_id,
        gender,
        buyer_age,
        flag
        from order_tb1_large

        where date(new_LS_start_time) between  (date('2022-01-01')) and (date('2022-03-31'))
        and   category_name in ('Travel & Luggage','Food & Beverages','Men Shoes','Home & Living','Women Shoes','Men Clothes','Women Clothes') 

)
select country, grass_date, new_category_id, category_name, sum(item_amount)as item_sold, sum(new_gmv_usd) as total_gmv_usd
from raw
group by 1,2,3,4
;
# --------------------------------------------------------------------
# --Table 1b: Category Order and Sales Performance
# --top 3 categories for each market based on orders
#--------------------------------------------------------------------

with raw as(
select distinct  country,
        date(cast(new_LS_start_time as datetime)) as grass_date,
        monthname(new_LS_start_time) as grass_month,
        new_order_ls_session_id,
        order_ls_streamer_type,
        order_cal_type,
        new_order_ls_streamer_id,
        item_amount,
        new_order_id,
        new_gmv_usd,
        category_name,
        new_category_id,
        new_buyer_id,
        new_order_item_id,
        new_order_model_id,
        gender,
        buyer_age,
        flag
        from order_tb1_large

        where date(new_LS_start_time) between  (date('2022-01-01')) and (date('2022-03-31'))
        and country in ('countryA','countryC', 'countryD','countryE','countryG')

)
,category_performace as 
(
    select  country
            ,grass_month
            ,new_category_id
            ,category_name
            ,sum(item_amount)as category_sold
            ,sum(new_gmv_usd) as total_gmv_usd
    from    raw
    group by 1, 2, 3, 4
)
,category_rank as 
(
    select  *
            ,rank()over(partition by country, grass_month order by category_sold desc) as selling_rank
    from    category_performace
)
select  *
from    category_rank
where   selling_rank < 4
;

#--------------------------------------------------------------------
# -- Table2. top 20 items within each region each category each month
# -- do not have item name, just has item id
#--------------------------------------------------------------------

with raw as(
select distinct  country,
        date(cast(new_LS_start_time as datetime)) as grass_date,
        monthname(new_LS_start_time) as grass_month,
        -- date_format((date_parse(cast(new_LS_start_time as varchar), '%Y-%m-%d %H:%i:%s.%f')), '%M') as grass_month,
        new_order_ls_session_id,
        order_ls_streamer_type,
        order_cal_type,
        new_order_ls_streamer_id,
        item_amount,
        new_order_id,
        new_gmv_usd,
        category_name,
        new_category_id,
        new_buyer_id,
        new_order_item_id,
        new_order_model_id,
        gender,
        buyer_age,
        flag
        from order_tb1_large

        where date(new_LS_start_time) between  (date('2022-01-01')) and (date('2022-03-31'))
        and country in ('countryA','countryC', 'countryD','countryE','countryG')
        and   category_name in ('Travel & Luggage','Food & Beverages','Men Shoes','Home & Living','Women Shoes','Men Clothes','Women Clothes') 
),
item_performace as 
(
    select  country
            ,grass_month
            ,new_category_id
            ,category_name
            ,new_order_item_id
            ,sum(item_amount)as item_sold
            ,sum(new_gmv_usd) as total_gmv_usd
    from    raw
    group by 1, 2, 3, 4, 5
),
sell_rank as(
select *, 
rank()over(partition by country
            ,grass_month
            ,new_category_id
            ,category_name order by item_sold desc, total_gmv_usd desc) as selling_rank
-- RANK() function adds the number of tied rows to the tied rank to calculate the rank of the next row, so the ranks may not be sequential. In addition, rows with the same values will get the same rank.
from item_performace
)
select *
from sell_rank
where selling_rank <21;


#--------------------------------------------------------------------
# -- Table 3: Frequency of purchase
# --3. daily purchase frequency per unique user
#--------------------------------------------------------------------
with raw as(
select distinct  country,
        date(cast(new_LS_start_time as datetime)) as grass_date,
        new_order_ls_session_id,
        order_ls_streamer_type,
        order_cal_type,
        new_order_ls_streamer_id,
        item_amount,
        new_order_id,
        new_gmv_usd,
        category_name,
        new_category_id,
        new_buyer_id,
        new_order_item_id,
        new_order_model_id,
        new_group_id,
        new_bundle_order_item_id,
        flag
        from order_table

        where date(new_LS_start_time) between  (date('2022-01-01')) and (date('2022-03-31'))
        and   category_name in ('Travel & Luggage','Food & Beverages','Men Shoes','Home & Living','Women Shoes','Men Clothes','Women Clothes') 

)
select 
    country, 
    grass_date, 
    new_category_id,
    category_name,  
    new_buyer_id as user_id, 
    sum(item_amount)as item_buy
from raw
group by 1,2,3,4,5
;

#------------------------------
#--Table 4: Optimal traffic
#------------------------------
with raw as(
select 
country,
grass_date,
grass_hour,
count(new_user_id) as num_user
from traffic_tb2
group by 1,2,3)
select country, grass_hour, avg(num_user)
from raw
group by 1,2;
#------------------------------
#--Table 4b: Optimal traffic
#------------------------------
select 
country,
grass_date,
grass_hour,
case when gender = 1 then 'male'
		when gender = 2 then 'female'
		when gender = 3 then 'male'
		when gender = 4 then 'female'
        end as genders,
count(new_user_id) as num_user
from traffic_tb2
group by 1,2,3,4;
#------------------------------
#--Table 5: Optimal traffic
#------------------------------
with raw as(
select 
country,
grass_date,
count(new_user_id) as num_user
from traffic_tb2
group by 1,2
)
select country,
weekday(grass_date),
avg(num_user)
from raw
group by 1,2;

#------------------------------
#--Table 6: target buyer age
#--also can use order table breakdown by different age level
#------------------------------

select 
country,
grass_date,
buyer_age,
count(new_user_id) as num_user
from traffic_tb2
group by 1,2,3;

