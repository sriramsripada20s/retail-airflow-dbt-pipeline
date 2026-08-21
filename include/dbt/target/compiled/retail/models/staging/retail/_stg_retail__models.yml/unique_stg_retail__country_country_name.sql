
    
    

select
    country_name as unique_field,
    count(*) as n_records

from RETAIL.TRANSFORM.stg_retail__country
where country_name is not null
group by country_name
having count(*) > 1


