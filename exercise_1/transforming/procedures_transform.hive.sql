set hive.cli.print.header=true;
-- First create the measures table.
-- The score_type field generated tells us what type of score it is
DROP TABLE IF EXISTS measures;
CREATE TABLE measures
    STORED AS PARQUET
    AS
SELECT measure_id as id, measure_name as name,
(CASE
      WHEN SUBSTR(measure_id,1,8) = 'SM_PART_' THEN 'Boolean'
      WHEN measure_id = 'OP_17' THEN 'Boolean'
      WHEN measure_id = 'OP_12' THEN 'Boolean'
      WHEN measure_id = 'OP_25' THEN 'Boolean'
      WHEN SUBSTR(measure_id,1,4) = 'HAI_' THEN 'Ratio'
      WHEN SUBSTR(measure_id,1,4) = 'PSI_' THEN 'Thousand'
      WHEN measure_id = 'VTE_6' THEN 'Number'
      WHEN measure_id = 'OP_18b' THEN 'Minutes'
      WHEN measure_id = 'OP_1' THEN 'Minutes'
      WHEN measure_id = 'ED_1b' THEN 'Minutes'
      WHEN measure_id = 'ED_2b' THEN 'Minutes'
      WHEN measure_id = 'OP_5' THEN 'Minutes'
      WHEN measure_id = 'OP_3b' THEN 'Minutes'
      WHEN measure_id = 'OP_20' THEN 'Minutes'
      WHEN measure_id = 'OP_21' THEN 'Minutes'
      ELSE 'Percent'
END) as score_type
FROM
 (SELECT measure_id, measure_name FROM raw_measures WHERE
   measure_id is not NULL and
   measure_id <> '' and
   measure_id <> 'ACS_REGISTRY' and
   measure_id <> 'PAYM_30_PN' and
   measure_id <> 'Star Rating' and
   measure_id <> 'MV' and
   measure_id <> 'PAYM_30_HF' and
   measure_id <> 'PSI_90' and
   measure_id <> 'HCAHPS' and
   measure_id <> 'PAYM_30_AMI' and
   measure_id <> 'MSPB_1' and
   measure_id <> 'OP_26' and
   measure_id <> 'EDV'
) measures_used
ORDER BY id;

-- Create intermediate tables for min and max values of score for
-- score_type Minutes, Number, Ratio
DROP TABLE IF EXISTS limits;
CREATE TABLE limits
    STORED AS PARQUET
    AS
SELECT r.measure_id, MIN(CAST(r.score AS DOUBLE)) AS scmin,
  MAX(CAST(r.score AS DOUBLE)) AS scmax
  FROM
  raw_effective_care r JOIN measures m ON (r.measure_id = m.id)
  WHERE (m.score_type = "Minutes" OR m.score_type = "Number"
    OR m.score_type = "Ratio")
    AND r.score is NOT NULL AND r.score <> "" AND r.score <> "Not Available"
  GROUP BY r.measure_id;


-- Next create the procedures table, using the measures table to determine
-- the type of score and convert the score.
DROP TABLE IF EXISTS procedures;
CREATE TABLE procedures
    STORED AS PARQUET
    AS
SELECT eff.provider_id, eff.measure_id, eff.normal_score
FROM (
    SELECT r.provider_id, r.measure_id, 
      (CASE
        WHEN LOWER(r.str_score) = 'y' THEN 100
        WHEN LOWER(r.str_score) = 'yes' THEN 100
        WHEN LOWER(r.str_score) = 'n' THEN 0
        WHEN LOWER(r.str_score) = 'no' THEN 0
        ELSE NULL
      END) normal_score
      FROM
        (SELECT provider_id, measure_id,
          score as str_score FROM raw_effective_care) r
      JOIN measures m
      ON (r.measure_id = m.id)
      WHERE m.score_type = "Boolean"
    UNION ALL
    SELECT r.provider_id, r.measure_id, r.thousand_score/1000 AS normal_score
      FROM
        (SELECT provider_id, measure_id,
          CAST(score AS DOUBLE) AS thousand_score FROM raw_effective_care) r
      JOIN measures m
      ON (r.measure_id = m.id)
      WHERE m.score_type = "Thousand"
    UNION ALL
    SELECT r.provider_id, r.measure_id,
      (CASE
-- Complement for types "Minutes", "Ratio" and "Number" since a
-- lower value is better
        WHEN l.scmin = l.scmax THEN 0
        ELSE 100 - (100 * ((r.numeric_score - l.scmin)/(l.scmax - l.scmin)))
      END) normal_score
      FROM
        (SELECT provider_id, measure_id,
          CAST(score AS DOUBLE) AS numeric_score FROM raw_effective_care) r
      JOIN measures m
      ON (r.measure_id = m.id)
      JOIN limits l
      ON (r.measure_id = l.measure_id)
      WHERE
        m.score_type = "Minutes" OR m.score_type = "Number"
          OR m.score_type = "Ratio"
    UNION ALL
    SELECT r.provider_id, r.measure_id, r.percent_score AS normal_score
      FROM
        (SELECT provider_id, measure_id,
          CAST(score AS DOUBLE) AS percent_score FROM raw_effective_care) r
      JOIN measures m
      ON (r.measure_id = m.id)
      WHERE m.score_type = "Percent"
    UNION ALL
    SELECT d.provider_id, d.measure_id, d.percent_score AS normal_score
      FROM
        (SELECT provider_id, measure_id,
-- Higher Mortality and readmission rates are not good, so take the complement
          (100 - CAST(score AS DOUBLE)) AS percent_score FROM raw_readmissions
        ) d
      JOIN measures m
      ON (d.measure_id = m.id)
      WHERE m.score_type = "Percent"
) eff
WHERE eff.provider_id  is NOT NULL AND eff.provider_id <> ''
AND eff.measure_id  is NOT NULL AND eff.measure_id <> ''
AND eff.normal_score is not NULL;
