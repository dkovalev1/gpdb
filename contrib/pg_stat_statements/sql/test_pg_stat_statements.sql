-- simple test to check there is no warnings during jumbling of the query
CREATE TABLE table_test_pg_stat_statements
(
    item1 integer,
    item2 integer,
    item3 integer,
    item4 integer
)
DISTRIBUTED BY (item1);

SELECT   
    SUM(item4) AS sum_item1
   ,item2
   ,GROUPING(item1)+GROUPING(item2) AS item_1_2_grouping
FROM
    table_test_pg_stat_statements
WHERE
    item3 = 0
GROUP BY ROLLUP(item1, item2);

DROP TABLE table_test_pg_stat_statements;
