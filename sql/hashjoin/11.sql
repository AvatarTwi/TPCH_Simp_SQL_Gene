EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON) SELECT * FROM orders join lineitem on l_orderkey = o_orderkey ;