EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON)  SELECT COUNT(*)  FROM nation  WHERE  n_name = 'GERMANY'  GROUP BY n_regionkey 