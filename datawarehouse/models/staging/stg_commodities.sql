with source as (

    select
        date,
        close_cl_f,
        close_gc_f,
        close_si_f
    from {{ source('dbsales_dyns', 'commodities') }}

),

unpivot as (

    select
        date,
        'CL=F' as simbolo,
        close_cl_f as valor_fechamento
    from source

    union all

    select
        date,
        'GC=F' as simbolo,
        close_gc_f
    from source

    union all

    select
        date,
        'SI=F' as simbolo,
        close_si_f
    from source
),

renamed as (

    select
        cast(date as date) as data,
        simbolo,
        valor_fechamento
    from unpivot
)

select * from renamed
