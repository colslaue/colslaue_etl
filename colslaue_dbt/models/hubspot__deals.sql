{{ config(schema='staging') }}

SELECT
    deal.id AS deal_id
    ,company.id AS company_id
    ,dealname AS deal_name
    ,DATE(deal.createdate) AS create_date
    ,DATE(deal.closedate) AS close_date
    ,pipeline AS pipeline
    ,dealstage AS deal_stage
    ,amount AS amount
    ,CONCAT(user.firstname, ' ', user.lastName) AS deal_owner
FROM {{ source('hubspot', 'deal') }} AS deal
LEFT JOIN {{ source('hubspot', 'deal_company') }} AS deal_company
ON deal.id = deal_company.deal_id
LEFT JOIN {{ source('hubspot', 'company') }} AS company
ON company.id = deal_company.companyid
LEFT JOIN {{ source('hubspot', 'user') }} AS user
ON user.id = deal.hubspot_owner_id
