EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON)  SELECT COUNT(*)  FROM lineitem  WHERE  l_shipdate <= date '1998-12-01' - interval '118' day  GROUP BY l_suppkey 