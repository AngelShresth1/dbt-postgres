/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/


with cte as(
SELECT 
    "id",
      cast("patient_id" as int),
      "status_code",
      "kind_code",
      "type_code",
      "type_display",
      "type_system",
      "policy_holder",
      "subscriber",
      "beneficiary",
      "dependent",
      "relationship_code",
      "relationship_display",
      "relationship_system",
      cast("start_date" as date),
      cast("end_date" as date),
      "insurer",
      "order" as "core_order",
      "network",
      "subrogation",
      "contract",
      "insurance_plan",
      "identifier_use",
      "identifier_type_code",
      "identifier_system",
      "identifier_value",
      "identifier_period_start",
      "identifier_period_end",
      "assigner",
      NULL as "data_source"
FROM {{ref('_raw_coverage')}}
union
select "enrollment_start_date" || '-' || "enrollment_end_date" || '-' || "dual_status_code" as "id",
      NULL as "patient_id",
      NULL as "status_code",
      NULL as "kind_code",
      CASE
        WHEN "dual_status_code" IS NOT NULL THEN "dual_status_code"
        ELSE '90'
      END as "type_code",
      CASE
        WHEN "dual_status_code" IS NOT NULL THEN 'dual_status_code'
        ELSE 'hardcoded'
      END as "type_display",
      CASE
        WHEN "dual_status_code" IS NOT NULL THEN 'dual_status_code'
        ELSE 'hardcoded'
      END as "type_system",
      NULL as "policy_holder",
      NULL as "subscriber",
      NULL as "beneficiary",
      NULL as "dependent",
      NULL as "relationship_code",
      NULL as "relationship_display",
      NULL as "relationship_system",
      "enrollment_start_date" as "start_date",
      "enrollment_end_date" as "end_date",
      NULL as "insurer",
      NULL as "core_order",
      NULL as "network",
      NULL as "subrogation",
      NULL as "contract",
      NULL as "insurance_plan",
      NULL as "identifier_use",
      "payer_type" as "identifier_type_code",
      NULL as "identifier_system",
      NULL as "identifier_value",
      NULL as "identifier_period_start",
      NULL as "identifier_period_end",
      NULL as "assigner",
      "data_source"
 from {{ref('_raw_eligibility')}}
 union
 select "enrollment_start_date" || '-' || "enrollment_end_date" || '-' || "medicare_status_code" as "id",
      NULL as "patient_id",
      NULL as "status_code",
      NULL as "kind_code",
      CASE
        WHEN "medicare_status_code" IS NOT NULL THEN "medicare_status_code"
        ELSE '90'
      END as "type_code",
      CASE
        WHEN "medicare_status_code" IS NOT NULL THEN 'medicare_status_code'
        ELSE 'hardcoded'
      END as "type_display",
      CASE
        WHEN "medicare_status_code" IS NOT NULL THEN 'medicare_status_code'
        ELSE 'hardcoded'
      END as "type_system",
      NULL as "policy_holder",
      NULL as "subscriber",
      NULL as "beneficiary",
      NULL as "dependent",
      NULL as "relationship_code",
      NULL as "relationship_display",
      NULL as "relationship_system",
      "enrollment_start_date" as "start_date",
      "enrollment_end_date" as "end_date",
      NULL as "insurer",
      NULL as "core_order",
      NULL as "network",
      NULL as "subrogation",
      NULL as "contract",
      NULL as "insurance_plan",
      NULL as "identifier_use",
      "payer_type" as "identifier_type_code",
      NULL as "identifier_system",
      NULL as "identifier_value",
      NULL as "identifier_period_start",
      NULL as "identifier_period_end",
      NULL as "assigner",
	"data_source"
 from {{ref('_raw_eligibility')}}
union
 select "enrollment_start_date" || '-' || "enrollment_end_date" || '-' || "original_reason_entitlement_code" as "id",
      NULL as "patient_id",
      NULL as "status_code",
      NULL as "kind_code",
      CASE
        WHEN "original_reason_entitlement_code" IS NOT NULL THEN "original_reason_entitlement_code"
        ELSE '90'
      END as "type_code",
      CASE
        WHEN "original_reason_entitlement_code" IS NOT NULL THEN 'original'
        ELSE 'hardcoded'
      END as "type_display",
      CASE
        WHEN "original_reason_entitlement_code" IS NOT NULL THEN 'original_reason_entitlement_code'
        ELSE 'hardcoded'
      END as "type_system",
      NULL as "policy_holder",
      NULL as "subscriber",
      NULL as "beneficiary",
      NULL as "dependent",
      NULL as "relationship_code",
      NULL as "relationship_display",
      NULL as "relationship_system",
      "enrollment_start_date" as "start_date",
      "enrollment_end_date" as "end_date",
      NULL as "insurer",
      NULL as "core_order",
      NULL as "network",
      NULL as "subrogation",
      NULL as "contract",
      NULL as "insurance_plan",
      NULL as "identifier_use",
      "payer_type" as "identifier_type_code",
      NULL as "identifier_system",
      NULL as "identifier_value",
      NULL as "identifier_period_start",
      NULL as "identifier_period_end",
      NULL as "assigner",
      "data_source"
 from {{ref('_raw_eligibility')}}
)
select * from cte