EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON)  SELECT *  FROM customer  WHERE  c_mktsegment = 'AUTOMOBILE'  ORDER BY c_nationkey 