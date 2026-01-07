with source as (

    select *
    from {{ source('dbsales_dyns', 'movimentacao_commodities') }}

),

renamed as (

    select
        date as data,
        symbol as simbolo,
        action as acao,
        quantity as quantidade
    from source

)

select *
from renamed
