WITH raw_metrics AS (

    SELECT
        reading ->> 'date' AS "date"
        , SPLIT_PART(_ab_source_file_url, '/', 3) AS person
        , reading ->> 'qty' AS qty
        , metric ->> 'name' AS metric_name
        , _ab_source_file_last_modified AS file_last_modified
    FROM
        {{ source('health', 'metrics') }},
        LATERAL JSONB_ARRAY_ELEMENTS(data -> 'metrics') AS metric,
        LATERAL JSONB_ARRAY_ELEMENTS(metric -> 'data') AS reading
    WHERE
        metric ->> 'name' = 'resting_heart_rate'

)

, latest_per_day AS (

    -- An export sent early in the day can carry a value that a later export revises,
    -- so keep one reading per person per day, from the newest file.
    SELECT DISTINCT ON (person, "date"::DATE)
        "date"
        , person
        , qty
        , metric_name
    FROM
        raw_metrics
    ORDER BY
        person
        , "date"::DATE
        , file_last_modified DESC
        , "date" DESC

)

SELECT
    "date"::TIMESTAMP AS record_date
    , person
    , qty::FLOAT AS resting_heart_rate
    , metric_name
FROM
    latest_per_day
