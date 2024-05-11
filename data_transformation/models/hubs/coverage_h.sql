/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/


{{ config(
    materialized='incremental',  
    incremental_strategy='merge',
    unique_key='id',
    on_schema_change='ignore'
) }}

{%- set source_model = "_stg_coverage" -%} 
{%- set src_pk = "uh_hk_hub_coverage" -%}
{%- set src_extra_columns = ["uh_data_space_id", "uh_last_updated_dt", "uh_data_run_id", "uh_applied_dt", "uh_biz_key_collision_cd"] -%}
{%- set src_nk = "id" -%}
{%- set src_ldts = "uh_load_dt" -%}
{%- set src_source = "uh_src" -%}

{{ automate_dv.hub(
    src_pk=src_pk, 
    src_nk=src_nk, 
    src_extra_columns=src_extra_columns, 
    src_ldts=src_ldts,
    src_source=src_source, 
    source_model=source_model
) }}
