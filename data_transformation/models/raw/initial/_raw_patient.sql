/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/



with ctePatient1
as
(
  select 
		"_AIRBYTE_DATA"::json -> 'address' -> 0 ->> 'city' as "city"
		, "_AIRBYTE_DATA"::json -> 'address' -> 0 -> 'line' ->> 0 as "line1"
	, "_AIRBYTE_DATA"::json -> 'address' -> 0 -> 'line' ->> 1 as "line2"
		, "_AIRBYTE_DATA"::json -> 'address' -> 0 ->> 'postalCode' as "postalcode"
	, "_AIRBYTE_DATA"::json -> 'address' -> 0 -> 'state' ->> 0 as "state"
	, "_AIRBYTE_DATA"::json ->> 'birthDate' as "birthdate"
	, "_AIRBYTE_DATA"::json ->> 'deceasedDateTime' as "deceaseddatetime"
	, "_AIRBYTE_DATA"::json ->> 'gender' as "gender"
	,"_AIRBYTE_DATA"::json ->> 'id' as "patient_id"
	, "_AIRBYTE_DATA"::json -> 'name' -> 0 ->> 'family' as "family"
		, "_AIRBYTE_DATA"::json -> 'name' -> 0 -> 'given' ->> 0 as "given"
		, "_AIRBYTE_DATA"::json -> 'name' -> 0 -> 'given' ->> 1 as "middlename"
	, "_AIRBYTE_DATA"::json -> 'name' -> 0 ->> 'use' as "name_use"
	, "_AIRBYTE_DATA"::json ->> 'resourceType' as "resource_type"
	, "_AIRBYTE_DATA"::json -> 'identifier' -> 1 -> 'period' ->> 'start' as "identifier_period_start"
	, "_AIRBYTE_DATA"::json -> 'identifier' -> 0 -> 'type' -> 'coding' -> 0 ->> 'code' as "identifier_typecode"
	, "_AIRBYTE_DATA"::json -> 'identifier' -> 0 -> 'type' -> 'coding' -> 0 ->> 'system' as "identifier_system"
	, "_AIRBYTE_DATA"::json -> 'identifier' -> 0 ->> 'value' as "identifier_value"
    from {{ source('landing', var('patient'))}}
	
    )
, ctePatient2
as
(
		select 
		"_AIRBYTE_DATA"::json -> 'address' -> 0 ->> 'city' as "city"
		, "_AIRBYTE_DATA"::json -> 'address' -> 0 -> 'line' ->> 0 as "line1"
	, "_AIRBYTE_DATA"::json -> 'address' -> 0 -> 'line' ->> 1 as "line2"
		, "_AIRBYTE_DATA"::json -> 'address' -> 0 ->> 'postalCode' as "postalcode"
	, "_AIRBYTE_DATA"::json -> 'address' -> 0 -> 'state' ->> 0 as "state"
	, "_AIRBYTE_DATA"::json ->> 'birthDate' as "birthdate"
	, "_AIRBYTE_DATA"::json ->> 'deceasedDateTime' as "deceaseddatetime"
	, "_AIRBYTE_DATA"::json ->> 'gender' as "gender"
	,"_AIRBYTE_DATA"::json ->> 'id' as "patient_id"
	, "_AIRBYTE_DATA"::json -> 'name' -> 0 ->> 'family' as "family"
		, "_AIRBYTE_DATA"::json -> 'name' -> 0 -> 'given' ->> 0 as "given"
		, "_AIRBYTE_DATA"::json -> 'name' -> 0 -> 'given' ->> 1 as "middlename"
	, "_AIRBYTE_DATA"::json -> 'name' -> 0 ->> 'use' as "name_use"
	, "_AIRBYTE_DATA"::json ->> 'resourceType' as "resource_type"
	, "_AIRBYTE_DATA"::json -> 'identifier' -> 1 -> 'period' ->> 'start' as "identifier_period_start"
	, "_AIRBYTE_DATA"::json -> 'identifier' -> 1 -> 'type' -> 'coding' -> 0 ->> 'code' as "identifier_typecode"
	, "_AIRBYTE_DATA"::json -> 'identifier' -> 1 -> 'type' -> 'coding' -> 0 ->> 'system' as "identifier_system"
	, "_AIRBYTE_DATA"::json -> 'identifier' -> 1 ->> 'value' as "identifier_value"
    from {{ source('landing', var('patient'))}}

	
    )
select 
ABS(cast("patient_id" as bigint)) as "patient_id"
    , null as "member_id"
      --CORE
    , null as "active"
    , "gender"
    , "birthdate"
    , null as "deceasedboolean"
    , "deceaseddatetime"
    , null as "maritalstatus_code"
    , null as "race"
    , null as "ethenicity"
    
    --MISC
    , null as "multiplebirth"
    , null as "contact"
    , null as "communication"
    , null as "generalpractitioner"
    , null as "managingorganization"
    , null as "misc_link"
    , null as "misc_extension"

    --IDENTIFIER
    , null as "identifier_use"
    , "identifier_typecode"
    , "identifier_system"
    , "identifier_value"
    , "identifier_period_start"
    , null as "identifier_period_end"
    , null as "assigner"

    --NAME
    , "name_use"
    , null as "name_text"
    , "family"
    , "given"
    , "middlename"
    , null as "prefix"
    , null as "suffix"
    , null as "name_period_start"
    , null as "name_period_end"


    --TELECOM
    , null as "telecom_system"
    , null as "telecom_value"
    , null as "telecom_use"
    , null as "rank"
    , null as "telecom_period_start"
    , null as "telecom_period_end"


    --ADDRESS
    , null as "address_use"
    , null as "address_type"
    , null as "address_text"
    , null as "address"
    , "line1"
    , "line2"
    , null as "line3"
    , null as "line4"
    , "city"
    , null as "district"
    , "state"
    , "postalcode"
    , null as "country"
    , null as "address_period_start"
    , null as "address_period_end"
    , "resource_type"
from ctepatient1
union
select 
ABS(cast("patient_id" as bigint)) as "patient_id"
    , null as "member_id"
      --CORE
    , null as "active"
    , "gender"
    , "birthdate"
    , null as "deceasedboolean"
    , "deceaseddatetime"
    , null as "maritalstatus_code"
    , null as "race"
    , null as "ethenicity"
    
    --MISC
    , null as "multiplebirth"
    , null as "contact"
    , null as "communication"
    , null as "generalpractitioner"
    , null as "managingorganization"
    , null as "misc_link"
    , null as "misc_extension"

    --IDENTIFIER
    , null as "identifier_use"
    , "identifier_typecode"
    , "identifier_system"
    , "identifier_value"
    , "identifier_period_start"
    , null as "identifier_period_end"
    , null as "assigner"

    --NAME
    , "name_use"
    , null as "name_text"
    , "family"
    , "given"
    , "middlename"
    , null as "prefix"
    , null as "suffix"
    , null as "name_period_start"
    , null as "name_period_end"


    --TELECOM
    , null as "telecom_system"
    , null as "telecom_value"
    , null as "telecom_use"
    , null as "rank"
    , null as "telecom_period_start"
    , null as "telecom_period_end"


    --ADDRESS
    , null as "address_use"
    , null as "address_type"
    , null as "address_text"
    , null as "address"
    , "line1"
    , "line2"
    , null as "line3"
    , null as "line4"
    , "city"
    , null as "district"
    , "state"
    , "postalcode"
    , null as "country"
    , null as "address_period_start"
    , null as "address_period_end"
    , "resource_type"
from ctepatient2
