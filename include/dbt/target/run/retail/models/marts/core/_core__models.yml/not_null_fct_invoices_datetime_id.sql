
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select datetime_id
from RETAIL.TRANSFORM.fct_invoices
where datetime_id is null



  
  
      
    ) dbt_internal_test