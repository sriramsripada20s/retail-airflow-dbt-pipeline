
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quantity
from RETAIL.TRANSFORM.int_retail__invoice_lines
where quantity is null



  
  
      
    ) dbt_internal_test