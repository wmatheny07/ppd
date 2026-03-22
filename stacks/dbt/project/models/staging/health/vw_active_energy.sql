SELECT
  id,
  person,
  data_source,
  qty,
  date,
  CASE
    WHEN LAG(mv_avg) OVER (
      PARTITION BY
        id
      ORDER BY
        date
    ) IS NOT NULL THEN (
      LAG(mv_avg) OVER (
        PARTITION BY
          id
        ORDER BY
          date
      ) - mv_avg
    ) / LAG(mv_avg) OVER (
      PARTITION BY
        id
      ORDER BY
        date
    )
    ELSE NULL
  END perc_diff_prev
FROM
  (
    SELECT
      id,
      person,
      data_source,
      qty,
      date,
      CASE
        WHEN ROW_NUMBER() OVER (
          PARTITION BY
            id
          ORDER BY
            date
        ) > 3 THEN AVG(qty::float) OVER (
          PARTITION BY
            id
          ORDER BY
            date ROWS BETWEEN 3 PRECEDING
            AND CURRENT ROW
        )
        ELSE NULL
      END mv_avg
    FROM
      (
        SELECT
          JSONB_ARRAY_ELEMENTS(data -> 'workouts') ->> 'id' AS id,
          split_part(_ab_source_file_url, '/', 3) person,
          JSONB_ARRAY_ELEMENTS(
            JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'activeEnergy'
          ) ->> 'qty' AS qty,
          JSONB_ARRAY_ELEMENTS(
            JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'activeEnergy'
          ) ->> 'date' AS date,
          JSONB_ARRAY_ELEMENTS(
            JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'activeEnergy'
          ) ->> 'units' AS units,
          JSONB_ARRAY_ELEMENTS(
            JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'activeEnergy'
          ) ->> 'source' AS data_source
        FROM
          {{ source('health', 'Workouts') }} w
      ) data
  ) data_mv
