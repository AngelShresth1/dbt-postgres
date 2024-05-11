/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/


WITH cteEligibility AS(
    SELECT cast("patient_id" as int),
	cast("member_id" as int),
	"enrollment_start_date" || '-' || "enrollment_end_date" as "id",
	"gender",
	"race",
	cast("birth_date" as date) as "birthdate",
	cast("death_date" as date) as "deceaseddatetime",
	cast("death_flag" as int) as "deceasedboolean",
	cast("enrollment_start_date" as date),
	cast("enrollment_end_date" as date),
	"payer",
	"payer_type",
	"plan",
	cast("original_reason_entitlement_code" as text),
	cast("dual_status_code" as text),
	cast("medicare_status_code" as text),
	"patient_id" || '-' || "race" as "given",
	"member_id" || '-' || "gender" as "family",
	"address" as "line1",
	"city",
	"state",
	"zip_code" as "postalcode",
	"phone" as "telecom_value",
	"data_source"
    FROM {{source("landing", var("eligibility"))}}
)
select * from cteEligibility