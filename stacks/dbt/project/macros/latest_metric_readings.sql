{#
  One metric's readings from health.metrics, with re-exports removed. A re-export can
  shift the seconds or change the source, which makes a row look new, so each minute
  comes only from the newest file containing it. Several readings in one minute of
  that file are all kept.

  metric_name: the metric's name in the export.
  fields:      reading keys to return. 'source' is returned as data_source.

  The MATERIALIZED CTE pulls the fields out before the sort. Without it the sort can
  carry each row's whole JSON array, which used over 350 GB of temp space in one rebuild.
#}
{% macro latest_metric_readings(metric_name, fields) %}
    WITH raw_readings AS MATERIALIZED (

        SELECT
            reading ->> 'date' AS "date"
            , SPLIT_PART(_ab_source_file_url, '/', 3) AS person
            {%- for field in fields %}
            , reading ->> '{{ field }}' AS {{ 'data_source' if field == 'source' else field }}
            {%- endfor %}
            , metric ->> 'name' AS metric_name
            , _ab_source_file_last_modified AS file_last_modified
        FROM
            {{ source('health', 'metrics') }},
            LATERAL JSONB_ARRAY_ELEMENTS(data -> 'metrics') AS metric,
            LATERAL JSONB_ARRAY_ELEMENTS(metric -> 'data') AS reading
        WHERE
            metric ->> 'name' = '{{ metric_name }}'

    )

    , ranked_by_file AS (

        SELECT
            *
            , RANK() OVER (
                PARTITION BY person, DATE_TRUNC('minute', "date"::TIMESTAMPTZ)
                ORDER BY file_last_modified DESC
            ) AS file_rank
        FROM
            raw_readings

    )

    SELECT DISTINCT
        "date"
        , person
        {%- for field in fields %}
        , {{ 'data_source' if field == 'source' else field }}
        {%- endfor %}
        , metric_name
    FROM
        ranked_by_file
    WHERE
        file_rank = 1
{% endmacro %}
