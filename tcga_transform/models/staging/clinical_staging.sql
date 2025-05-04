{{ config(
    materialized='view',
    schema='staging'
) }}

WITH cleaned_data AS (
    SELECT
        -- Basic file and case info
        NULLIF(TRIM(file_id), '') AS file_id,
        NULLIF(TRIM(file_name), '') AS file_name,
        NULLIF(TRIM(case_id), '') AS case_id,
        NULLIF(TRIM(case_submitter_id), '') AS case_submitter_id,

        -- Demographic info
        NULLIF(TRIM(gender), '') AS gender,
        NULLIF(TRIM(race), '') AS race,
        NULLIF(TRIM(ethnicity), '') AS ethnicity,
        CASE
            WHEN year_of_birth IS NOT NULL
            AND year_of_birth BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE)
            THEN year_of_birth
            ELSE NULL
        END AS year_of_birth,

        -- Clinical info
        NULLIF(TRIM(primary_diagnosis), '') AS primary_diagnosis,
        NULLIF(TRIM(tissue_or_organ_of_origin), '') AS tissue_or_organ_of_origin,

        -- Keep nested structures for further processing
        samples
    FROM {{ source('raw', 'methylation_metadata') }}
    WHERE
        -- Basic data quality checks
        file_id IS NOT NULL
        AND case_id IS NOT NULL
)
SELECT
    *,
    -- Calculate age at data collection (approximate)
    CASE
        WHEN year_of_birth IS NOT NULL
        THEN EXTRACT(YEAR FROM CURRENT_DATE) - year_of_birth
        ELSE NULL
    END AS age_at_collection
FROM cleaned_data