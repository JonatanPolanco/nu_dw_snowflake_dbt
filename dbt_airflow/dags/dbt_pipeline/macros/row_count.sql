{% test row_count(model, above=0, below=none) %}
  WITH row_stats AS (
    SELECT 
      COUNT(*) AS actual_count
    FROM {{ model }}
  )
  SELECT 
    actual_count,
    {{ above }} AS min_expected,
    {% if below %}{{ below }}{% else %}NULL{% endif %} AS max_expected,
    '{{ model.name }}' AS model_name,
    CASE 
      WHEN actual_count < {{ above }} THEN 'TOO_FEW_ROWS'
      {% if below %}
      WHEN actual_count > {{ below }} THEN 'TOO_MANY_ROWS'
      {% endif %}
    END AS error_type  -- Opcional: tipo de error
  FROM row_stats
  WHERE 
    actual_count < {{ above }}
    {% if below %}
    OR actual_count > {{ below }}
    {% endif %}
{% endtest %}