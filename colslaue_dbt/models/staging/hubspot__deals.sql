WITH
	primary_company AS(
		SELECT
			*
		FROM {{ source('hubspot', 'company') }} AS company
		LEFT JOIN {{ source('hubspot', 'deal_company') }} AS deal_company
		ON company.id = deal_company.companyid
		WHERE type = 'deal_to_company'
	)

SELECT
    deal.id AS deal_id
    ,dealname AS deal_name
    ,CONCAT(user.firstname, ' ', user.lastName) AS deal_owner
    ,DATE(deal.createdate) AS create_date
    ,DATE(deal.closedate) AS close_date
    ,pipeline AS pipeline
    ,dealstage AS deal_stage
    ,amount AS amount
    ,company.id AS primary_company_id
    ,company.name AS primary_company_name
FROM {{ source('hubspot', 'deal') }} AS deal
LEFT JOIN {{ source('hubspot', 'user') }} AS user
ON user.id = deal.hubspot_owner_id
LEFT JOIN primary_company AS company
ON company.deal_id = deal.id
