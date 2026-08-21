
    
    

select
    datetime_id as unique_field,
    count(*) as n_records

from RETAIL.TRANSFORM.dim_datetime
where datetime_id is not null
group by datetime_id
having count(*) > 1


