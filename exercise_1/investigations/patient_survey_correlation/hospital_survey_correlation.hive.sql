set hive.cli.print.header=true;
SELECT
  corr(h.rank,s.rank) corr_coeff
FROM
  (SELECT 
    havg.provider_id,
    rank() over (ORDER BY havg.avg_score DESC) as rank,
    havg.names as name
  FROM
    (SELECT
      hp.provider_id,
      collect_set(hp.name) as names,
      avg(hp.score) as avg_score,
      count(distinct(hp.measure_id)) as num_procs
    FROM
      (SELECT h.provider_id, h.name, p.measure_id, p.normal_score as score
      FROM hospitals h JOIN procedures p
        ON (h.provider_id = p.provider_id)
      ) hp
    GROUP BY hp.provider_id
    ) havg
--  WHERE havg.num_procs >= 8
  ) h
JOIN
  (SELECT 
-- Since multiple hospitals can get the highest survey score(100)
-- order alphabetically as well  
    havg.provider_id,
    rank() over (ORDER BY havg.avg_score DESC, havg.names ASC) as rank,
    havg.names as name
  FROM
    (SELECT
      h.provider_id,
      collect_set(h.name) as names,
      avg(ps.survey_score) as avg_score
    FROM
      hospitals h   
    JOIN
      surveys ps
    ON (ps.provider_id = h.provider_id)
    GROUP BY h.provider_id
    ) havg
  ) s
ON (h.provider_id = s.provider_id)
