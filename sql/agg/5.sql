EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON) SELECT COUNT(*) FROM partsupp GROUP BY ps_partkey ;