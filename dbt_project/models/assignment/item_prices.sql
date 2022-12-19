{{
    config(
        materialized='incremental',
        unique_key = ['Version','country_code','Item_key','year','month','user']
    )
}}

with fetch_data_with_country as (
    select * from {{source('country_mapping','country_codes')}} cross join {{source('item_details','item_prices_stg')}}

)

select Version, Country_key as country_code , Country,Item_key,Item_name,year,month,price,user
from fetch_data_with_country


