{{ config(schema='staging') }}

SELECT
    id AS deal_id
    ,dealname AS deal_name
    ,DATE(createdate) AS create_date
    ,DATE(closedate) AS close_date
    ,pipeline AS pipeline
    ,dealstage AS deal_stage
    ,amount AS amount
FROM {{ source('hubspot', 'deal') }}