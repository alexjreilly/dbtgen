{{
    config(
        materialized='view'
    )
}}


SELECT * FROM  {{ source('sfdc', '${name}') }}
