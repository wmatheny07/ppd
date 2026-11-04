{{ config(
    materialized='incremental',
    unique_key=['record_date', 'person'],
    incremental_strategy='delete+insert'
) }}

WITH raw_metrics AS MATERIALIZED (

    SELECT
        reading ->> 'date' AS "date"
        , SPLIT_PART(_ab_source_file_url, '/', 3) AS person
        , reading ->> 'rem' AS rem
        , reading ->> 'core' AS core
        , reading ->> 'deep' AS deep
        , reading ->> 'awake' AS awake
        , reading ->> 'asleep' AS asleep
        , reading ->> 'inBedStart' AS in_bed_start
        , reading ->> 'sleepStart' AS sleep_start
        , reading ->> 'inBedEnd' AS in_bed_end
        , reading ->> 'sleepEnd' AS sleep_end
        , _ab_source_file_last_modified AS file_last_modified
    FROM
        {{ source('health', 'metrics') }},
        LATERAL JSONB_ARRAY_ELEMENTS(data -> 'metrics') AS metric,
        LATERAL JSONB_ARRAY_ELEMENTS(metric -> 'data') AS reading
    WHERE
        metric ->> 'name' = 'sleep_analysis'

)

, most_complete_per_day AS (

    -- A night can appear in several exports, and neither the first nor the newest is
    -- reliably complete. The first is often an AutoSleep row with no stages, and later
    -- ones can cover only part of the night. So keep the row with the most staged sleep,
    -- falling back to the longest asleep-only row, then the newest file.
    SELECT DISTINCT ON (person, "date"::DATE)
        *
    FROM
        raw_metrics
    ORDER BY
        person
        , "date"::DATE
        , COALESCE(rem::FLOAT, 0) + COALESCE(core::FLOAT, 0) + COALESCE(deep::FLOAT, 0) DESC
        , COALESCE(asleep::FLOAT, 0) DESC
        , file_last_modified DESC

)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'most_complete_per_day.date',
        'most_complete_per_day.person'
    ]) }} AS id
    , most_complete_per_day."date"::DATE AS record_date
    , most_complete_per_day.person
    , most_complete_per_day.rem::FLOAT AS rem
    , most_complete_per_day.core::FLOAT AS core
    , most_complete_per_day.deep::FLOAT AS deep
    , most_complete_per_day.awake::FLOAT AS awake
    , COALESCE(most_complete_per_day.asleep::FLOAT, 0) AS asleep
    , most_complete_per_day.in_bed_start::TIMESTAMP AS in_bed_start
    , most_complete_per_day.sleep_start::TIMESTAMP AS sleep_start
    , most_complete_per_day.in_bed_end::TIMESTAMP AS in_bed_end
    , most_complete_per_day.sleep_end::TIMESTAMP AS sleep_end
    -- Includes awake time because the Superset efficiency metrics subtract it from this.
    , COALESCE(most_complete_per_day.rem::FLOAT, 0)
        + COALESCE(most_complete_per_day.core::FLOAT, 0)
        + COALESCE(most_complete_per_day.deep::FLOAT, 0)
        + COALESCE(most_complete_per_day.awake::FLOAT, 0)
        + COALESCE(most_complete_per_day.asleep::FLOAT, 0) AS total_sleep
FROM
    most_complete_per_day
{%- if is_incremental() %}
LEFT JOIN (
    SELECT person, MAX(record_date) AS max_record_date
    FROM {{ this }}
    GROUP BY person
) AS watermark
    ON watermark.person = most_complete_per_day.person
WHERE
    -- Re-check 30 days so a later or re-sent export can correct a night.
    watermark.max_record_date IS NULL
    OR most_complete_per_day."date"::DATE > watermark.max_record_date - INTERVAL '30 days'
{% endif %}
