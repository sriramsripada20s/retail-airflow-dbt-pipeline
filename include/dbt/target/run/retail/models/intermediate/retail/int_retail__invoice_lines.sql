
  create or replace   view RETAIL.TRANSFORM.int_retail__invoice_lines
  
  
  
  
  as (
    -- int_retail__invoice_lines.sql
-- Computes line-level totals and filters to valid, sellable rows.
-- This is where "what counts as a real sale" logic lives, kept separate
-- from both the raw staging pass-through and the dimensional marts.

with invoices as (
    select * from RETAIL.TRANSFORM.stg_retail__invoices
),

priced as (
    select
        *,
        quantity * unit_price as line_total
    from invoices
    where quantity > 0        -- excludes returns/cancellations (negative quantity)
      and unit_price > 0      -- excludes free-item / data-entry-error rows
      and stock_code is not null
)

select * from priced
  );

