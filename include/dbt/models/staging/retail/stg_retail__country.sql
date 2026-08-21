-- stg_retail__country.sql
with source as (
    select * from {{ source('retail', 'country') }}
),

renamed as (
    select
        id::integer         as country_id,
        iso::string          as iso,
        name::string         as country_name,
        nicename::string     as country_nicename,
        iso3::string         as iso3,
        numcode::integer     as numcode,
        phonecode::integer   as phonecode
    from source
)

select * from renamed