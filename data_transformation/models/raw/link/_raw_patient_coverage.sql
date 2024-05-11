/*__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
*/


SELECT 
    p.patient_id,
	p.member_id,
	c.id
from {{ ref('_raw_patient_eligibility')}} p
    join {{ref('_raw_coverage_eligibility')}} c
    on p.patient_id=c.patient_id