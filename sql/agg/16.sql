EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON) SELECT COUNT(*) FROM nation GROUP BY n_nationkey ;