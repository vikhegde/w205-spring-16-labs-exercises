set hive.cli.print.header=true;
DROP TABLE IF EXISTS surveys;
CREATE TABLE surveys
    STORED AS PARQUET
    AS
SELECT
  provider_id,
  (100 * (CAST(t.comm_nurse_tokens[0][0] AS DOUBLE) /
    CAST(t.comm_nurse_tokens[0][3] AS DOUBLE))) AS comm_nurse_score,
  (100 * (CAST(t.comm_doc_tokens[0][0] AS DOUBLE) /
    CAST(t.comm_doc_tokens[0][3] AS DOUBLE))) AS comm_doc_score,
  (100 * (CAST(t.resp_staff_tokens[0][0] AS DOUBLE) /
    CAST(t.resp_staff_tokens[0][3] AS DOUBLE))) AS resp_staff_score,
  (100 * (CAST(t.pain_mgmt_tokens[0][0] AS DOUBLE) /
    CAST(t.pain_mgmt_tokens[0][3] AS DOUBLE))) AS pain_mgmt_score,
  (100 * (CAST(t.comm_med_tokens[0][0] AS DOUBLE) /
    CAST(t.comm_med_tokens[0][3] AS DOUBLE))) AS comm_med_score,
  (100 * (CAST(t.clean_quiet_tokens[0][0] AS DOUBLE) /
    CAST(t.clean_quiet_tokens[0][3] AS DOUBLE))) AS clean_quiet_score,
  (100 * (CAST(t.discharge_info_tokens[0][0] AS DOUBLE) /
    CAST(t.discharge_info_tokens[0][3] AS DOUBLE))) AS discharge_info_score,
  survey_score
FROM
  (SELECT provider_id,
    SENTENCES(comm_nurse_achieve) AS comm_nurse_tokens,
    SENTENCES(comm_doc_achieve) AS comm_doc_tokens,
    SENTENCES(response_staff_achieve) AS resp_staff_tokens,
    SENTENCES(pain_mgmt_achieve) AS pain_mgmt_tokens,
    SENTENCES(comm_med_achieve) AS comm_med_tokens,
    SENTENCES(clean_quiet_achieve) AS clean_quiet_tokens,
    SENTENCES(discharge_info_achieve) AS discharge_info_tokens,
    CAST(hcahps_base_score AS DOUBLE)
      + CAST(hcahps_consistency_score AS DOUBLE) AS survey_score
  FROM raw_survey_responses
  WHERE
    provider_id is not NULL
    AND provider_id <> '' 
    AND provider_id <> "Not Available"
    AND comm_nurse_achieve is not NULL
    AND comm_nurse_achieve <> ''
    AND comm_nurse_achieve <> "Not Available"
    AND comm_doc_achieve is not NULL
    AND comm_doc_achieve <> ''
    AND comm_doc_achieve <> "Not Available"
    AND response_staff_achieve is not NULL
    AND response_staff_achieve <> ''
    AND response_staff_achieve <> "Not Available"
    AND pain_mgmt_achieve is not NULL
    AND pain_mgmt_achieve <> ''
    AND pain_mgmt_achieve <> "Not Available"
    AND comm_med_achieve is not NULL
    AND comm_med_achieve <> ''
    AND comm_med_achieve <> "Not Available"
    AND clean_quiet_achieve is not NULL
    AND clean_quiet_achieve <> ''
    AND clean_quiet_achieve <> "Not Available"
    AND discharge_info_achieve is not NULL
    AND discharge_info_achieve <> ''
    AND discharge_info_achieve <> "Not Available"
    AND hcahps_base_score is not NULL
    AND hcahps_base_score <> ''
    AND hcahps_base_score <> "Not Available"
    AND hcahps_consistency_score is not NULL
    AND hcahps_consistency_score <> ''
    AND hcahps_consistency_score <> "Not Available"
  ) t
ORDER BY provider_id;
