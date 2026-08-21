
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select line_total
from RETAIL.TRANSFORM.int_retail__invoice_lines
where line_total is null



  
  
      
    ) dbt_internal_test