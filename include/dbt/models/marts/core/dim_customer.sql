-- dim_customer.sql
-- Builds the customer dimension table with a surrogate key, ISO country mapping,
-- and a default sentinel row for guest/anonymous purchases.

-- Step 1: Extract unique customer and country combinations from clean invoice line items
with customers as (
    select distinct
        customer_id,
        country
    from {{ ref('int_retail__invoice_lines') }}
    -- Exclude missing customer IDs here; guest purchases are handled separately below
    where customer_id is not null
),

-- Step 2: Load country mapping reference table
country as (
    select * from {{ ref('stg_retail__country') }}
),

-- Step 3: Map known customers to their ISO country codes and generate a primary surrogate key
known_customers as (
    select
        -- Hash customer_id and country into a deterministic primary surrogate key
        {{ dbt_utils.generate_surrogate_key(['customers.customer_id', 'customers.country']) }} as customer_key,
        customers.customer_id,
        customers.country,
        country.iso
    from customers
    -- Join on standardized country name to obtain official ISO country codes
    left join country
        on upper(customers.country) = upper(country.country_name)
),

-- Step 4: Define a default sentinel record (-1) for guest / anonymous transactions.
-- Without this, joining fct_invoices on CustomerID would drop ~25% of unassigned revenue lines.
unknown_customer as (
    select
        '-1'      as customer_key,  -- Standard default key used across dimensional models
        null      as customer_id,
        'UNKNOWN' as country,
        null      as iso
)

-- Step 5: Combine known customer records with the anonymous default record
select * from known_customers
union all
select * from unknown_customer