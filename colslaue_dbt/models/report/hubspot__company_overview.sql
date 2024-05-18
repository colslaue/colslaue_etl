WITH
	deal AS(
		SELECT
			deal_id
			,primary_company_id
			,amount
		FROM {{ ref('hubspot__deals') }}
	)
SELECT
	company.company_id AS company_id
	,AVG(deal.amount) AS average_deal_size
FROM {{ ref('hubspot__companies') }} AS company
LEFT JOIN deal
ON deal.primary_company_id = company.company_id
GROUP BY 1