/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/


{{ config(
    materialized='table',
) }}

{%- set source_model = "_stg_patient" -%}
{%- set src_pk = ["uh_hk_hub_patient", "uh_hk_telecom_diff", "uh_load_dt"] -%}
{%- set src_extra_columns = ["uh_data_space_id", "uh_data_run_id", "uh_applied_dt", "uh_seq_id"] -%}
{%- set src_hashdiff = "uh_hk_telecom_diff" -%}
{%- set src_payload = ["telecom_system",
    "telecom_value",
    "telecom_use",
    "rank",
    "telecom_period_start",
    "telecom_period_end"] -%}
{%- set src_source = "uh_src" -%}
{%- set src_eff = "uh_applied_dt" -%}
{%- set src_ldts = "uh_load_dt" -%}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                   src_payload=src_payload, 
                   src_extra_columns=src_extra_columns,
                   src_eff=src_eff,
                   src_ldts=src_ldts,
                   src_source=src_source,
                   source_model=source_model) }}
