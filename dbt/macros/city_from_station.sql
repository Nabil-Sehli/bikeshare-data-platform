{#
    Citi Bike runs one GBFS system for New York and Jersey City/Hoboken.
    JC/Hoboken stations have short names prefixed JC / HB (regions 70 and 311).
#}
{% macro city_from_station(short_name_col, region_id_col=none) -%}
    case
        when {{ short_name_col }} like 'JC%' or {{ short_name_col }} like 'HB%' then 'JC'
        {%- if region_id_col %}
        when {{ region_id_col }} in ('70', '311') then 'JC'
        {%- endif %}
        else 'NYC'
    end
{%- endmacro %}
