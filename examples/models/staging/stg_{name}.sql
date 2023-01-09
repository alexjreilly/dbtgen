{{
    config(
        materialized='view'
    )
}}

{{
    generate_staging_model(
        src=ref('base_${name}'),
        unique_columns=${unique_columns},
        unique_key='id'
    )
}}
