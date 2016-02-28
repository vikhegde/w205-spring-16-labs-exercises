set hive.cli.print.header=true;
SELECT 
  rank() over (ORDER BY agg.avg_rank ASC) as agg_rank,
  agg.names as name,
  agg.agg_score as agg_score,
  agg.avg_score as avg_score,
  agg.score_stddev as score_stddev
  FROM
  (SELECT
    collect_set(rm.name) as names,
    avg(rm.rank) as avg_rank,
    sum(rm.score) as agg_score,
    avg(rm.score) as avg_score,
    stddev_pop(rm.score) as score_stddev,
    count(distinct(rm.measure_id)) as num_procs
    FROM
    (SELECT hp.provider_id, hp.name, hp.score, hp.measure_id,
      rank() OVER (PARTITION BY hp.measure_id ORDER BY hp.score DESC) as rank 
      FROM
        (SELECT h.provider_id, h.name, p.measure_id, p.normal_score as score
          FROM hospitals h JOIN procedures p
          ON (h.provider_id = p.provider_id)
        ) hp
    ) rm
    GROUP BY rm.provider_id
  ) agg WHERE agg.num_procs >= 8 ORDER BY agg_rank ASC LIMIT 10;
