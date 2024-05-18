SELECT
	id AS user_id
	,firstname AS first_name
	,lastname AS last_name
	,CONCAT(firstname, " ", lastname) AS full_name
	,email
FROM {{ source('hubspot', 'user') }}