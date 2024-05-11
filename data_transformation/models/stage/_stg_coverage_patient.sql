/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/



{%- set yaml_metadata -%}
source_model: '_raw_patient_coverage'
derived_columns:
  uh_data_space_id: '!1'
  uh_src: '!xyz'
  uh_data_run_id: '!101'
  uh_biz_key_collision_cd: '!56'
  uh_seq_id: '!95'
  uh_hk_hub_patient: 'patient_id'
  uh_hk_hub_coverage: 'id'
hashed_columns:
  uh_hk_hub_patient: 'patient_id'
  uh_hk_hub_coverage: 'id'
  uh_hk_patient_coverage:
    is_hashdiff: true
    columns:
      - 'uh_hk_hub_patient'
      - 'uh_hk_hub_coverage'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

WITH staging AS (
{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}
)


SELECT 
    *,
    CAST(current_timestamp AS timestamp) AS uh_load_dt,
    CAST(current_timestamp AS timestamp) AS uh_applied_dt,
    CAST(current_timestamp AS timestamp) AS uh_last_updated_dt
FROM staging
