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
          workout ->> 'id' AS id,
          split_part(w._ab_source_file_url, '/', 3) AS person,
          {{ convert_energy(
            "energy_entry ->> 'qty'",
            "energy_entry ->> 'units'"
          ) }} AS qty,
          energy_entry ->> 'date' AS date,
          energy_entry ->> 'units' AS units,
          energy_entry ->> 'source' AS data_source
        FROM
          {{ source('health', 'Workouts') }} w,
          LATERAL JSONB_ARRAY_ELEMENTS(w.data -> 'workouts') AS workout,
          LATERAL JSONB_ARRAY_ELEMENTS(workout -> 'activeEnergy') AS energy_entry
      ) data
  ) data_mv
