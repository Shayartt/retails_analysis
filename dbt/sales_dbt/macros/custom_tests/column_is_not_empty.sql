-- macros/custom_tests/column_is_not_empty.sql

{% test column_is_not_empty(model, column_name) %}
WITH validation AS (
    SELECT
        *
    FROM {{ model }}
    WHERE {{ column_name }} = ''
       OR {{ column_name }} IS NULL
)
SELECT
    COUNT(*)
FROM validation
{% endtest %}
