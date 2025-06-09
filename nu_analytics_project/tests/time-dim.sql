WITH date_spine AS (
    SELECT 
        DATE_TRUNC('day', DATEADD('day', row_number() OVER (ORDER BY NULL) - 1, '2020-01-01'::DATE)) AS expected_date
    FROM TABLE(GENERATOR(ROWCOUNT => 2000))  -- ~5 years of dates
),

missing_dates AS (
    SELECT ds.expected_date
    FROM date_spine ds
    LEFT JOIN {{ ref('base_time_dimension') }} td ON ds.expected_date = td.utc_date
    WHERE td.utc_date IS NULL
      AND ds.expected_date <= CURRENT_DATE()
)

SELECT * FROM missing_dates