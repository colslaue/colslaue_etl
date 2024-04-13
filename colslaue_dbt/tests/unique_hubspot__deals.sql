SELECT
	deal_id
FROM {{ ref('hubspot__deals') }}
GROUP BY deal_id
HAVING COUNT(*) > 1