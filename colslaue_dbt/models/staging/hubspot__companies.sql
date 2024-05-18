SELECT
	company.id AS company_id
	,DATE(company.createdate, "America/Halifax") AS create_date
	,user.full_name AS company_owner
	,company.country
	,company.annualrevenue AS annual_revenue
	,company.name AS company_name
FROM {{ source('hubspot', 'company') }} AS company
LEFT JOIN {{ ref('hubspot__users') }} AS user
ON user.user_id = company.hubspot_owner_id