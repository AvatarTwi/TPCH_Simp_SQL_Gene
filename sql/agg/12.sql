EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON) 
SELECT COUNT(*) FROM lineitem GROUP BY l_linenumber ;