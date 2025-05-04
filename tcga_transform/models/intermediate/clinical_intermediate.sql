{{ config(
    materialized='view',
    schema='intermediate'
) }}

WITH flattened_samples AS (
    SELECT
        s.file_id,
        s.case_id,
        s.case_submitter_id,
        s.gender,
        s.race,
        s.ethnicity,
        s.year_of_birth,
        s.age_at_collection,
        s.primary_diagnosis,
        s.tissue_or_organ_of_origin,
        sample.sample_id,
        sample.sample_submitter_id,
        NULLIF(TRIM(sample.sample_type), '') AS sample_type,
        NULLIF(TRIM(sample.composition), '') AS sample_composition,
        CASE
            WHEN sample.days_to_collection >= 0
            THEN sample.days_to_collection
            ELSE NULL
        END AS days_to_collection,
        sample.analytes
    FROM {{ ref('clinical_staging') }} s,
    UNNEST(samples) AS sample
),
flattened_analytes AS (
    SELECT
        fs.*,
        analyte.analyte_id,
        NULLIF(TRIM(analyte.analyte_type), '') AS analyte_type,
        analyte.aliquots
    FROM flattened_samples fs,
    UNNEST(analytes) AS analyte
),
flattened_aliquots AS (
    SELECT
        fa.*,
        aliquot.aliquot_id
    FROM flattened_analytes fa,
    UNNEST(aliquots) AS aliquot
)
SELECT
    *,
    -- Add a unique row identifier
    CONCAT(file_id, '_', sample_id, '_', analyte_id, '_', aliquot_id) AS unique_row_id
FROM flattened_aliquots
WHERE
    sample_id IS NOT NULL
    AND analyte_id IS NOT NULL
    AND aliquot_id IS NOT NULL