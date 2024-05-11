/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/




with cteCoverage as (

    SELECT 
          "_AIRBYTE_DATA"::json ->> 'id' as "id"
        , "_AIRBYTE_DATA"::json ->> 'status' as "status_code"
        , "_AIRBYTE_DATA"::json -> 'payor' -> 0 -> 'identifier' ->> 'value' as "identifier_value" 
        , "_AIRBYTE_DATA"::json -> 'type' -> 'coding' -> 0 ->> 'code' as "type_code"
        , "_AIRBYTE_DATA"::json -> 'type' -> 'coding' -> 0 ->> 'code' as "type_system"
    FROM {{source("landing", var('coverage'))}}

)
select 
      "id",
      NULL as "patient_id",
      "status_code",
      NULL as "kind_code",
      "type_code",
      "type_system" as "type_display",
      "type_system",
      NULL as "policy_holder",
      NULL as "subscriber",
      NULL as "beneficiary",
      NULL as "dependent",
      NULL as "relationship_code",
      NULL as "relationship_display",
      NULL as "relationship_system",
      NULL as "start_date",
      NULL as "end_date",
      NULL as "insurer",
      NULL as "order",
      NULL as "network",
      NULL as "subrogation",
      NULL as "contract",
      NULL as "insurance_plan",
      NULL as "identifier_use",
      NULL as "identifier_type_code",
      CASE
        WHEN "identifier_value" IS NOT NULL THEN 'payor'
        ELSE NULL
        END as "identifier_system",
        "identifier_value",
      NULL as "identifier_period_start",
      NULL as "identifier_period_end",
      NULL as "assigner"

from ctecoverage