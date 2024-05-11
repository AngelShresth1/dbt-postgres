-- Custom SQL test to check if all orders have a corresponding customer ID
SELECT 
    COUNT(*) AS missing_source_patient_id
FROM 
    ref(_raw_lab)
WHERE 
    source_patient_id IS NULL
