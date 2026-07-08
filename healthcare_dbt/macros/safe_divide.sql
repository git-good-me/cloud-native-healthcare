/*
  safe_divide
  ===========
  Divides numerator by denominator, returning null instead of
  crashing when denominator is zero or null.

  Usage:
    {{ safe_divide('total_patients', 'total_beds') }}
*/

{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null then null
        else {{ numerator }} / cast({{ denominator }} as double)
    end
{% endmacro %}