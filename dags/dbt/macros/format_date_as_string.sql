{# Format a date column to date string #}

{% macro format_date_as_string(date_column, format_string="YYYYMMDD") -%}

    TO_CHAR({{ dbt.safe_cast(date_column, api.Column.translate_type('date')) }}, '{{ format_string }}')

{%- endmacro %}