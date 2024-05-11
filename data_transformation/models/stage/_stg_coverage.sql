/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/


{%- set yaml_metadata -%}
source_model: '_raw_coverage_eligibility'
derived_columns:
  uh_data_space_id: '!1'
  uh_src: '!bcda'
  uh_data_run_id: '!101'
  uh_biz_key_collision_cd: '!56'
  uh_seq_id: '!95'
hashed_columns:
  uh_hk_hub_covergae: 'id'
  uh_hk_core_diff:
    is_hashdiff: true
    columns:
      - 'status_code'
      - 'kind_code'
      - 'type_code'
      - 'type_display'
      - 'type_system'
      - 'policy_holder'
      - 'subscriber'
      - 'beneficiary'
      - 'dependent'
      - 'relationship_code'
      - 'relationship_display'
      - 'relationship_system'
      - 'start_date'
      - 'end_date'
      - 'insurer'
      - 'core_order'
      - 'network'
      - 'subrogation'
      - 'contract'
      - 'insurance_plan'
  uh_hk_identifier_diff:
    is_hashdiff: true
    columns:
      - 'identifier_use'
      - 'identifier_type_code'
      - 'identifier_system'
      - 'identifier_value'
      - 'identifier_period_start'
      - 'identifier_period_end'
      - 'assigner'
  
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}


with staging as (
{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}
)


SELECT 
    CAST(id as varchar(100)) as id,
    CAST(patient_id AS VARCHAR(40)) AS patient_id,
    CAST(status_code AS VARCHAR(20)) AS status_code,
    CAST(kind_code AS VARCHAR(20)) AS kind_code,
    CAST(type_code AS VARCHAR(20)) as type_code,
    CAST(type_display AS VARCHAR(300)) AS type_display,
    CAST(type_system AS VARCHAR(300)) AS type_system,
    CAST(policy_holder AS varchar(16)) AS policy_holder,
    CAST(subscriber AS varchar(16)) AS subscriber,
    CAST(beneficiary AS varchar(16)) AS beneficiary,
    CAST(dependent AS VARCHAR(100)) AS dependent,
    CAST(relationship_code AS VARCHAR(20)) AS relationship_code,
    CAST(relationship_display AS VARCHAR(100)) AS relationship_display,
    CAST(relationship_system AS VARCHAR(100)) AS relationship_system,
    CAST(start_date AS DATE) AS start_date,
    CAST(end_date AS DATE) AS end_date,
    CAST(insurer AS varchar(16)) AS insurer,
    CAST(core_order AS INT) AS core_order,
    CAST(network AS VARCHAR(100)) AS network,
    CAST(subrogation AS BOOLEAN) AS subrogation,
    CAST(contract AS varchar(16)) AS contract,
    CAST(insurance_plan AS varchar(16)) AS insurance_plan,
    CAST(identifier_use AS VARCHAR(40)) AS identifier_use,
    CAST(identifier_type_code AS VARCHAR(20)) AS identifier_type_code,
    CAST(identifier_system AS VARCHAR(100)) AS identifier_system,
    CAST(identifier_value AS VARCHAR(100)) AS identifier_value,
    CAST(identifier_period_start AS timestamp) AS identifier_period_start,
    CAST(identifier_period_end AS timestamp) AS identifier_period_end,
    CAST(assigner AS VARCHAR(100)) AS assigner,
    CAST(data_source AS VARCHAR(100)) AS data_source,
    CAST(uh_hk_hub_covergae as varchar(20)) as uh_hk_hub_coverage,
    CAST(uh_data_space_id as varchar(20)) as uh_data_space_id,
    CAST(current_timestamp as timestamp) as uh_load_dt,
    CAST(CASE
        WHEN data_source IS NOT NULL THEN data_source
        ELSE uh_src
    END as varchar(100)) as uh_src,
    CAST(current_timestamp as timestamp) as uh_applied_dt,
    CAST(current_timestamp as timestamp) as uh_last_updated_dt,
    CAST(uh_data_run_id as varchar(40)) as uh_data_run_id,
    CAST(uh_biz_key_collision_cd as varchar(20)) as uh_biz_key_collision_cd,
    CAST(uh_seq_id as numeric(38,0)) as uh_seq_id,
    CAST(uh_hk_core_diff as varchar(20)) as uh_hk_core_diff,
    CAST(uh_hk_identifier_diff as varchar(20)) as uh_hk_identifier_diff
FROM
    staging
