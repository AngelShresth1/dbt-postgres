/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}