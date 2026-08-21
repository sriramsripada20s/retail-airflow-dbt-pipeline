-- stg_retail__invoices.sql
-- 1:1 with the raw source: rename to snake_case, cast types, no joins/aggregation.

with source as (
    select * from RETAIL.RAW.raw_invoices
),

renamed as (
    select
        InvoiceNo::string                                      as invoice_no,
        StockCode::string                                      as stock_code,
        Description::string                                    as description,
        Quantity::integer                                      as quantity,
        -- As our dataset contains multiple timestamp formats (e.g., 2-digit years mixed with 4-digit years), you can chain TRY_TO_TIMESTAMP with COALESCE
        COALESCE(
        TRY_TO_TIMESTAMP(InvoiceDate, 'MM/DD/YY HH24:MI'),
        TRY_TO_TIMESTAMP(InvoiceDate, 'MM/DD/YYYY HH24:MI'),
        TRY_TO_TIMESTAMP(InvoiceDate, 'YYYY-MM-DD HH24:MI:SS')
        ) AS invoice_datetime,
        UnitPrice::float                                       as unit_price,
        CustomerID::integer                                    as customer_id,
        Country::string                                        as country
    from source
)

select * from renamed