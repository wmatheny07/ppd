WITH raw_metrics AS (

    {{ latest_metric_readings('walking_running_distance', ['qty', 'source']) }}

)

SELECT
    "date"::TIMESTAMP AS record_date
    , person
    , qty::FLOAT AS distance
    , data_source
    , metric_name
FROM
    raw_metrics
WHERE
    metric_name = 'walking_running_distance'
