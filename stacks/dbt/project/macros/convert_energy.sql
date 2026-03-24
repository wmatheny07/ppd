{#
  Convert an energy quantity to kcal.
  If units = 'kJ', divides by 4.184; otherwise casts as-is.
  Both arguments are expressions that resolve to text/varchar.
#}
{% macro convert_energy(qty, units) %}
  CASE
    WHEN ({{ units }}) = 'kJ' THEN ({{ qty }})::decimal(10, 3) / 4.184
    ELSE ({{ qty }})::decimal(10, 3)
  END
{% endmacro %}
