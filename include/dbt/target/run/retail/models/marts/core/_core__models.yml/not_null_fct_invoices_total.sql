
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total
from RETAIL.TRANSFORM.fct_invoices
where total is null



  
  
      
    ) dbt_internal_test