EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON)  SELECT *  FROM orders  JOIN lineitem  ON o_orderkey = l_orderkey  WHERE  o_comment LIKE '%special%' 