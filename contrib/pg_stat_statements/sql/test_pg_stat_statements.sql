-- start_ignore
\! gpconfig -c shared_preload_libraries -v 'pg_stat_statements';
\! gpstop -raq -M fast;
\c
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
DROP TABLE IF EXISTS table_test_pg_stat_statements;
-- end_ignore
-- simple test to check there is no warnings during jumbling of the query
CREATE TABLE table_test_pg_stat_statements
(
    item1 integer,
    item2 integer,
    item3 integer,
    item4 integer
)
DISTRIBUTED BY (item1);

-- Known issue: query is not added to pg_stat_statements statistics in
-- case it is planned by GPORCA. So disable GPORCA during tests.
SET optimizer='off';

SELECT pg_stat_statements_reset();

-- launch 2 equivalent queries  
SELECT
    GROUPING(item1)
FROM
    table_test_pg_stat_statements
WHERE
    item3 = 0
GROUP BY ROLLUP(item1, item2);

SELECT
    GROUPING(item1)
FROM
    table_test_pg_stat_statements
WHERE
    item3 = 1
GROUP BY ROLLUP(item1, item2);

-- pg_stat_statements statistics should have 2 calls for 1 entry
-- corresponding to the two preceding SELECT queries
SELECT query, calls FROM pg_stat_statements ORDER BY QUERY;

-- launch not equivalent query
SELECT
    GROUPING(item2)
FROM
    table_test_pg_stat_statements
WHERE
    item3 = 0
GROUP BY ROLLUP(item1, item2);

-- check that it has separate entry
SELECT query, calls FROM pg_stat_statements ORDER BY QUERY;

-- check that different grouping options result in separate entries
SELECT
    COUNT (*)
FROM
    table_test_pg_stat_statements
GROUP BY ROLLUP(item1, item2, item3, item4);

SELECT
    COUNT (*)
FROM
    table_test_pg_stat_statements
GROUP BY CUBE(item1, item2, item3, item4);

SELECT
    COUNT (*)
FROM
    table_test_pg_stat_statements
GROUP BY GROUPING SETS(item1, item2, item3, item4);

SELECT
    COUNT (*)
FROM
    table_test_pg_stat_statements
GROUP BY GROUPING SETS((item1, item2), (item3, item4));

SELECT
    COUNT (*)
FROM
    table_test_pg_stat_statements
GROUP BY item1, item2, item3, item4;

SELECT query, calls FROM pg_stat_statements ORDER BY QUERY;

-- check several parameters options in ROLLUP
-- all should result in separate entries
SELECT pg_stat_statements_reset();

SELECT
    COUNT (*)
FROM
    table_test_pg_stat_statements
GROUP BY ROLLUP(item1, item2);

SELECT
    COUNT (*)
FROM
    table_test_pg_stat_statements
GROUP BY ROLLUP(item2, item3);

SELECT query, calls FROM pg_stat_statements ORDER BY QUERY;

RESET optimizer;

DROP TABLE table_test_pg_stat_statements;
-- start_ignore
\! gpconfig -r shared_preload_libraries;
\! gpstop -raq -M fast;
-- end_ignore
