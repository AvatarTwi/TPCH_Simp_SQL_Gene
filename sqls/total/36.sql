EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON)  SELECT *  FROM customer  JOIN nation  ON c_nationkey = n_nationkey  WHERE  c_mktsegment = 'AUTOMOBILE' 