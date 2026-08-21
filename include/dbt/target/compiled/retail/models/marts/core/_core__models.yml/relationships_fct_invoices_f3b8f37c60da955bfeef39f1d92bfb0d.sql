
    
    

with child as (
    select datetime_id as from_field
    from RETAIL.TRANSFORM.fct_invoices
    where datetime_id is not null
),

parent as (
    select datetime_id as to_field
    from RETAIL.TRANSFORM.dim_datetime
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


