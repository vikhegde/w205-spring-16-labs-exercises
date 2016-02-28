set hive.cli.print.header=true;

SELECT
  src, median_stddev, avg_stddev, min_stddev, max_stddev
FROM
  (SELECT
    "Procedure" as src,
    PERCENTILE_APPROX(pstd.p_stddev, 0.5) as median_stddev,
    AVG(pstd.p_stddev) as avg_stddev,
    MIN(pstd.p_stddev) as min_stddev,
    MAX(pstd.p_stddev) as max_stddev
  FROM
    (SELECT
      p.measure_id,
      stddev_pop(p.normal_score) as p_stddev
    FROM
      procedures p
    GROUP BY p.measure_id
    ) pstd
  UNION ALL
  SELECT
    "Survey" as src,
    PERCENTILE_APPROX(sstd.p_stddev, 0.5) as median_stddev,
    AVG(sstd.p_stddev) as avg_stddev,
    MIN(sstd.p_stddev) as min_stddev,
    MAX(sstd.p_stddev) as max_stddev
  FROM
    (SELECT
      stddev_pop(comm_nurse_score) as p_stddev
    FROM
      surveys
    UNION ALL
    SELECT
      stddev_pop(comm_doc_score) as p_stddev
    FROM
      surveys
    UNION ALL
    SELECT
      stddev_pop(resp_staff_score) as p_stddev
    FROM
      surveys
    UNION ALL
    SELECT
      stddev_pop(pain_mgmt_score) as p_stddev
    FROM
      surveys
    UNION ALL
    SELECT
      stddev_pop(comm_med_score) as p_stddev
    FROM
      surveys
    UNION ALL
    SELECT
      stddev_pop(clean_quiet_score) as p_stddev
    FROM
      surveys
    UNION ALL
    SELECT
      stddev_pop(discharge_info_score) as p_stddev
    FROM
      surveys
    ) sstd
  ) stdstats;
