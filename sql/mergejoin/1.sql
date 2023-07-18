EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON)
SELECT * FROM nation join customer on c_nationkey = n_nationkey order by c_nationkey ;