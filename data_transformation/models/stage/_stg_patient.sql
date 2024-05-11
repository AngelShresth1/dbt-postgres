/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/

{%- set yaml_metadata -%}
source_model: "_raw_patient_eligibility"
derived_columns:
  uh_data_space_id: "!1"
  uh_src: "!bcda"
  uh_data_run_id: "!101"
  uh_biz_key_collision_cd: "!56"
  uh_seq_id: "!95"
hashed_columns:
  uh_hk_hub_patient: "patient_id"
  uh_hk_hub_member: "member_id"
  uh_hk_core_diff:
    is_hashdiff: true
    columns:
      - "active"
      - "gender"
      - "birthdate"
      - "deceasedboolean"
      - "deceaseddatetime"
      - "maritalstatus_code"
      - "race"
      - "ethenicity"
  uh_hk_misc_diff:
    is_hashdiff: true
    columns:
      - "multiplebirth"
      - "contact"
      - "communication"
      - "generalpractitioner"
      - "managingorganization"
      - "misc_link"
      - "misc_extension"
  uh_hk_identifier_diff:
    is_hashdiff: true
    columns:
      - "identifier_use"
      - "identifier_typecode"
      - "identifier_system"
      - "identifier_value"
      - "identifier_period_start"
      - "identifier_period_end"
      - "assigner"
  uh_hk_name_diff:
    is_hashdiff: true
    columns:
      - "name_use"
      - "name_text"
      - "family"
      - "given"
      - "middlename"
      - "prefix"
      - "suffix"
      - "name_period_start"
      - "name_period_end"
  uh_hk_telecom_diff:
    is_hashdiff: true
    columns:
      - "telecom_system"
      - "telecom_value"
      - "telecom_use"
      - "rank"
      - "telecom_period_start"
      - "telecom_period_end"
  uh_hk_address_diff:
    is_hashdiff: true
    columns:
      - "address_use"
      - "address_type"
      - "address_text"
      - "line1"
      - "line2"
      - "line3"
      - "line4"
      - "city"
      - "district"
      - "state"
      - "postalcode"
      - "country"
      - "address_period_start"
      - "address_period_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict["source_model"] %}

{% set derived_columns = metadata_dict["derived_columns"] %}

{% set hashed_columns = metadata_dict["hashed_columns"] %}


with staging as (
{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}
)


  select 
    cast("patient_id" as varchar(40)) as "patient_id",
    cast("member_id" as varchar(40)) as "member_id",
    cast("active" as boolean) as "active",
    cast("gender" as varchar(40)) as "gender",
    cast("birthdate" as date) as "birthdate",
    cast("deceasedboolean" as boolean) as "deceasedboolean",
    cast("deceaseddatetime" as timestamp) as "deceaseddatetime",
    cast("maritalstatus_code" as varchar(20)) as "maritalstatus_code",
    cast("race" as varchar(50)) as "race",
    cast("ethenicity" as varchar(20)) as "ethenicity",
    cast("multiplebirth" as varchar(100)) as "multiplebirth",
    cast("contact" as varchar(100)) as "contact",
    cast("communication" as varchar(100)) as "communication",
    cast("generalpractitioner" as varchar(100)) as "generalpractitioner",
    cast("managingorganization" as varchar(100)) as "managingorganization",
    cast("misc_link" as varchar(100)) as "misc_link",
    cast("misc_extension" as varchar(100)) as "misc_extension",
    cast("identifier_use" as varchar(40)) as "identifier_use",
    cast("identifier_typecode" as varchar(20)) as "identifier_typecode",
    cast("identifier_system" as varchar(100)) as "identifier_system",
    cast("identifier_value" as varchar(100)) as "identifier_value",
    cast("identifier_period_start" as timestamp) as "identifier_period_start",
    cast("identifier_period_end" as timestamp) as "identifier_period_end",
    cast("assigner" as varchar(100)) as "assigner",
    cast("name_use" as varchar(40)) as "name_use",
    cast("name_text" as varchar(300)) as "name_text",
    cast("family" as varchar(300)) as "family",
    cast("given" as varchar(300)) as "given",
    cast("middlename" as varchar(300)) as "middlename",
    cast("prefix" as varchar(300)) as "prefix",
    cast("suffix" as varchar(300)) as "suffix",
    cast("name_period_start" as timestamp) as "name_period_start",
    cast("name_period_end" as timestamp) as "name_period_end",
    cast("telecom_system" as varchar(40)) as "telecom_system",
    cast("telecom_value" as varchar(300)) as "telecom_value",
    cast("telecom_use" as varchar(40)) as "telecom_use",
    cast("rank" as int) as "rank",
    cast("telecom_period_start" as timestamp) as "telecom_period_start",
    cast("telecom_period_end" as timestamp) as "telecom_period_end",
    cast("address_use" as varchar(40)) as address_use,
    cast("address_type" as varchar(40)) as address_type,
    cast("address_text" as varchar(300)) as address_text,
    cast("line1" as varchar(100)) as "line1",
    cast("line2" as varchar(100)) as "line2",
    cast("line3" as varchar(100)) as "line3",
    cast("line4" as varchar(100)) as "line4",
    cast("city" as varchar(100)) as "city",
    cast("district" as varchar(100)) as "district",
    cast("state" as varchar(100)) as "state",
    cast("postalcode" as varchar(100)) as "postalcode",
    cast("country" as varchar(100)) as "country",
    cast("address_period_start" as timestamp) as "address_period_start",
    cast("address_period_end" as timestamp) as "address_period_end",
    cast("resource_type" as varchar(100)) as "resource_type",
    cast("data_source" as varchar(100)) as "data_source",
    cast("uh_hk_hub_patient" as varchar(20)) as "uh_hk_hub_patient",
    cast("uh_hk_hub_member" as varchar(20)) as "uh_hk_hub_member",
    cast("uh_data_space_id" as varchar(20)) as "uh_data_space_id",
    cast(current_timestamp as timestamp) as "uh_load_dt",
    cast(case
        when "data_source" is not null then "data_source"
        else "uh_src"
    end as varchar(100)) as "uh_src",
    cast(current_timestamp as timestamp) as "uh_applied_dt",
    cast(current_timestamp as timestamp) as "uh_last_updated_dt",
    cast("uh_data_run_id" as varchar(40)) as "uh_data_run_id",
    cast("uh_biz_key_collision_cd" as varchar(20)) as "uh_biz_key_collision_cd",
    cast("uh_seq_id" as numeric(38,0)) as "uh_seq_id",
    cast("uh_hk_address_diff" as varchar(20)) as "uh_hk_address_diff",
    cast("uh_hk_telecom_diff" as varchar(20)) as "uh_hk_telecom_diff",
    cast("uh_hk_core_diff" as varchar(20)) as "uh_hk_core_diff",
    cast("uh_hk_misc_diff" as varchar(20)) as "uh_hk_misc_diff",
    cast("uh_hk_identifier_diff" as varchar(20)) as "uh_hk_identifier_diff",
    cast("uh_hk_name_diff" as varchar(20)) as "uh_hk_name_diff"
from staging
