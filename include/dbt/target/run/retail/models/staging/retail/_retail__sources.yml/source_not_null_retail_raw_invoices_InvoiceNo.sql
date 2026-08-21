
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select InvoiceNo
from RETAIL.RAW.raw_invoices
where InvoiceNo is null



  
  
      
    ) dbt_internal_test