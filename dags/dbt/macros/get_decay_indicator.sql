{# This macro returns the indicator for decay type. #}

{% macro get_decay_indicator(decay_type) -%}

    case {{ dbt.safe_cast(decay_type, api.Column.translate_type("integer")) }}
        when 0 then 'Non Decay'
        when 1 then 'Decay'
        else 'Not Identify'
    end

{%- endmacro %}