EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON)  SELECT *  FROM nation  JOIN customer  ON n_nationkey = c_nationkey  WHERE  n_name = 'GERMANY' 