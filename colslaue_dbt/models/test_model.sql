SELECT
    *
FROM {{ source('staging', 'test_table') }}