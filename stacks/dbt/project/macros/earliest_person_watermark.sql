{#
  The latest loaded value for whichever person is furthest behind, for incremental filters.
  A single MAX across everyone would skip a person's data once the other person's newer
  exports moved past it. Reprocessing the other person's rows is harmless because these
  models replace rows by key.
#}
{% macro earliest_person_watermark(expression) %}
    (
        SELECT MIN(person_max)
        FROM (
            SELECT MAX({{ expression }}) AS person_max
            FROM {{ this }}
            GROUP BY person
        ) AS person_watermarks
    )
{% endmacro %}
