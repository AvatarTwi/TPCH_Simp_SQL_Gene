EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON) SELECT COUNT(*) FROM customer GROUP BY c_nationkey ;