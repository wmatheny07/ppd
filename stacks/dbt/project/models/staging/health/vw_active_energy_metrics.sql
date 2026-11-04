WITH raw_metrics AS (

    {{ latest_metric_readings('active_energy', ['qty', 'units', 'source']) }}

)

SELECT
    "date"::TIMESTAMP AS record_date
    , person
    , {{ convert_energy("qty", "units") }}::FLOAT AS calories
    , data_source
    , metric_name
FROM
    raw_metrics
WHERE
    metric_name = 'active_energy'
