EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON)  SELECT *  FROM orders  JOIN lineitem  ON o_orderkey = l_orderkey  WHERE  o_orderdate <= date '1998-12-01' - interval '78' day  ORDER BY l_orderkey 