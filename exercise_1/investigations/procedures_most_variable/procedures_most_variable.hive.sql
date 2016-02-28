set hive.cli.print.header=true;
SELECT
  rank() over (ORDER BY ps.p_stddev DESC) as rank,
  ps.p_name as procedure_name,
  ps.p_stddev as score_stddev,
  ps.p_sum as agg_score,
  ps.p_avg as avg_score
FROM
  (SELECT
    collect_set(pm.name) as p_name,
    stddev_pop(pm.score) as p_stddev,
    sum(pm.score) as p_sum,
    avg(pm.score) as p_avg
  FROM
    (SELECT 
      m.name,
      m.id,
      p.normal_score AS score
    FROM
      procedures p JOIN measures m
        ON (p.measure_id = m.id)
    ) pm
  GROUP BY pm.id
  ) ps
ORDER BY rank ASC LIMIT 10
