{{ config(
    materialized='table',
    schema='marts',
    partition_by={
        "field": "year_of_birth",
        "data_type": "integer",
        "range": {
            "start": 1900,
            "end": 2025,
            "interval": 10
        }
    },
    cluster_by=['primary_diagnosis', 'sample_type']
) }}

SELECT
    unique_row_id,
    file_id,
    case_id,
    case_submitter_id,
    gender,
    race,
    ethnicity,
    year_of_birth,
    age_at_collection,
    primary_diagnosis,
    tissue_or_organ_of_origin,
    sample_id,
    sample_submitter_id,
    sample_type,
    sample_composition,
    days_to_collection,
    analyte_id,
    analyte_type,
    aliquot_id,
    -- Add derived fields
    CASE
        WHEN days_to_collection IS NOT NULL
        THEN days_to_collection / 365.25
        ELSE NULL
    END AS years_to_collection,
    -- Standardize gender values
    CASE
        WHEN LOWER(gender) IN ('male', 'm') THEN 'Male'
        WHEN LOWER(gender) IN ('female', 'f') THEN 'Female'
        ELSE 'Unknown'
    END AS standardized_gender
FROM {{ ref('clinical_intermediate') }}
WHERE
    -- Final data quality checks
    age_at_collection IS NULL OR age_at_collection BETWEEN 0 AND 120
    AND (days_to_collection IS NULL OR days_to_collection >= 0)