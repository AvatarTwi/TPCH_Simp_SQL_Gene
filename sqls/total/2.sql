EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON)  SELECT COUNT(*)  FROM customer  WHERE  c_mktsegment IN ('AUTOMOBILE','AUTOMOBILE')  GROUP BY c_nationkey 