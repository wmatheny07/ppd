WITH raw_metrics AS (

    {{ latest_metric_readings('time_in_daylight', ['qty']) }}

)

SELECT
    "date"::TIMESTAMP AS record_date
    , person
    , qty::FLOAT AS time_in_daylight
    , metric_name
FROM
    raw_metrics
WHERE
    metric_name = 'time_in_daylight'
