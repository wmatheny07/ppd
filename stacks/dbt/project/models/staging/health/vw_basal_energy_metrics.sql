WITH raw_metrics AS (

    {{ latest_metric_readings('basal_energy_burned', ['qty', 'units', 'source']) }}

)

SELECT
    "date"::TIMESTAMP AS record_date
    , person
    , {{ convert_energy("qty", "units") }}::FLOAT AS basal_energy_burned
    , data_source
    , metric_name
FROM
    raw_metrics
WHERE
    metric_name = 'basal_energy_burned'
