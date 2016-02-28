set hive.cli.print.header=true;
SELECT
  rank() over (ORDER BY agg.avg_rank ASC) as rank,
  agg.state AS state,
  agg.agg_score AS agg_score,
  agg.avg_score AS avg_score,
  agg.score_stddev AS score_stddev
FROM
  (SELECT
    r.state,
    avg(r.rank) as avg_rank,
    avg(r.m_avg) as avg_score,
    sum(r.m_sum) as agg_score,
    stddev_pop(r.m_avg) as score_stddev,
    count(distinct(r.measure_id)) as num_procs
  FROM
    (SELECT
      sp.state,
      sp.measure_id,
      sp.m_sum,
      sp.m_avg,
      rank() OVER (PARTITION BY sp.measure_id ORDER BY sp.m_avg DESC) as rank
    FROM
      (SELECT
        h.state,
        p.measure_id,
        avg(p.normal_score) as m_avg, 
        sum(p.normal_score) as m_sum
      FROM
        hospitals h JOIN procedures p ON (h.provider_id = p.provider_id)
      GROUP BY h.state, p.measure_id
      ) sp
    ) r
  GROUP BY r.state
  ) agg
WHERE agg.num_procs >= 8 ORDER BY rank ASC LIMIT 10;
