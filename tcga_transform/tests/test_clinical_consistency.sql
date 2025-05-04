-- depends_on: {{ ref('stg_clinical') }}

SELECT
  sample_id,
  case_id
FROM {{ ref('stg_clinical') }}
WHERE case_id IS NULL