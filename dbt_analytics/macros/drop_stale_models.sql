{% macro drop_stale_models(database=target.project, schema=target.dataset, days=7, dry_run=True) %}
   {{ log('Generating cleanup queries ...') }}
   {% set get_drop_stale_table_commands_query %}
    WITH tables_since_modified_dates AS (
        SELECT
            table_id,
            DATE_DIFF(CURRENT_DATE(), DATE(TIMESTAMP_MILLIS(last_modified_time)), DAY) AS days_since_modified
        FROM {{ database }}.{{ schema }}.__TABLES__
        ),
        table_types AS (
            SELECT 
                table_name, 
                table_type,
                CASE
                    WHEN table_type = 'VIEW' THEN table_type
                    ELSE 'TABLE'
                END AS drop_type,
            FROM {{ database }}.{{ schema }}.INFORMATION_SCHEMA.TABLES
        )
        SELECT 
            t.table_name,
            'DROP ' || drop_type || ' IF EXISTS ' || '{{ database }}.' || '{{ schema }}.' ||  d.table_id || ';' AS drop_query
        FROM tables_since_modified_dates d
        JOIN table_types t ON t.table_name = d.table_id
        WHERE days_since_modified >= {{ days }}
    {% endset %}

    {{ log(stale_table_query, info=True) }}

    {% set drop_queries = run_query(get_drop_stale_table_commands_query).columns[1].values() %}
    
    {% for query in drop_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}
        {% else %}
            {{ log('Dropping object with command: ' ~ query, info=True) }}
            {% do run_query(query)%}
        {% endif %}
    {% endfor %}
{% endmacro %}

{#
-- How to execute the macro?
-- dbt run-operation drop_stale_models --args '{'schema': 'dbt_raustralia', 'days': 2, 'dry_run': 'True'}'
#}
