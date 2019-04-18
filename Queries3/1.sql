SELECT SUM(ps_supplycost), ps_suppkey
FROM partsupp
WHERE ps_supplycost > 0
GROUP BY ps_suppkey
