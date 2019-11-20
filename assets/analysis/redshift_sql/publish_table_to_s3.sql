UNLOAD ('
  SELECT {{ columns|join(', ') }} FROM (
    SELECT 1 AS ix, {% for column in columns %} \'{{ column }}\' AS {{ column }} {% if not loop.last %}, {% endif %} {% endfor %}
    UNION
    SELECT 2 AS ix, {% for column in columns %} CAST({{ column }} AS VARCHAR(255)) AS {{ column }} {% if not loop.last %}, {% endif %} {% endfor %}
    FROM {{ table_name }}
  ) ORDER BY ix;
')
TO 's3://{{ published_bucket_name }}/{{ path }}/{{ table_name }}.csv'
IAM_ROLE '{{ redshift_role_arn }}'
ALLOWOVERWRITE
PARALLEL OFF
DELIMITER ',';
