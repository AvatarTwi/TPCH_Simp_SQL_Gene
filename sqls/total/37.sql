EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON)  SELECT *  FROM customer  JOIN supplier  ON c_nationkey = s_nationkey  WHERE  c_mktsegment IN ('AUTOMOBILE','AUTOMOBILE') 