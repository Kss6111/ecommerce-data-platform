{% macro date_spine_columns(timestamp_col) %}
    DATE({{ timestamp_col }})                                   AS date_day,
    DATE_TRUNC('week',  {{ timestamp_col }})                    AS date_week,
    DATE_TRUNC('month', {{ timestamp_col }})                    AS date_month,
    DATE_TRUNC('year',  {{ timestamp_col }})                    AS date_year,
    YEAR({{ timestamp_col }})                                   AS year_num,
    MONTH({{ timestamp_col }})                                  AS month_num,
    DAY({{ timestamp_col }})                                    AS day_num,
    DAYOFWEEK({{ timestamp_col }})                              AS day_of_week,
    CASE DAYOFWEEK({{ timestamp_col }})
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END                                                         AS day_name
{% endmacro %}