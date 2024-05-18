WITH
	primary_company AS(
		SELECT
			company.company_id
			,company.company_name
			,deal_company.deal_id
		FROM {{ ref('hubspot__companies') }} AS company
		LEFT JOIN {{ source('hubspot', 'deal_company') }} AS deal_company
		ON company.company_id = deal_company.companyid
		WHERE type = 'deal_to_company'
	)

SELECT
    deal.id AS deal_id
    ,dealname AS deal_name
    ,user.full_name AS deal_owner
    ,DATE(deal.createdate) AS create_date
    ,DATE(deal.closedate) AS close_date
    ,pipeline AS pipeline
    ,dealstage AS deal_stage
    ,amount AS amount
    ,company.company_id AS primary_company_id
    ,company.company_name AS primary_company_name
FROM {{ source('hubspot', 'deal') }} AS deal
LEFT JOIN {{ ref('hubspot__users') }} AS user
ON user.user_id = deal.hubspot_owner_id
LEFT JOIN primary_company AS company
ON company.deal_id = deal.id
