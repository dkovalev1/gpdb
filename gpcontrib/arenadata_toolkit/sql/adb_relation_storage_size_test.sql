CREATE EXTENSION arenadata_toolkit;

CREATE TABLE heap_table_with_toast(a INT, b TEXT)
DISTRIBUTED BY (a);

CREATE TABLE heap_table_without_toast(a INT, b INT)
DISTRIBUTED BY (a);

CREATE TABLE ao_table_with_toast(a INT, b TEXT)
WITH (APPENDOPTIMIZED=true)
DISTRIBUTED BY (a);

CREATE TABLE ao_table_without_toast(a INT, b INT)
WITH (APPENDOPTIMIZED=true)
DISTRIBUTED BY (a);

-- Check that toast exists only for "with_toast" tables
SELECT relname, reltoastrelid != 0 with_toast
FROM pg_class
WHERE relname IN ('heap_table_with_toast', 'heap_table_without_toast',
                  'ao_table_with_toast', 'ao_table_without_toast')
ORDER BY 1;

-- Insert initial data to tables
INSERT INTO heap_table_with_toast SELECT i, 'short_text' FROM generate_series(1,15) AS i;
INSERT INTO heap_table_without_toast SELECT i, i*10 FROM generate_series(1,15) AS i;
INSERT INTO ao_table_with_toast SELECT i, 'short_text' FROM generate_series(1,15) AS i;
INSERT INTO ao_table_without_toast SELECT i, i*10 FROM generate_series(1,15) AS i;

-- Check sizes on segments
SELECT relname, sizes.gp_segment_id, sizes.size
FROM pg_class, arenadata_toolkit.adb_relation_storage_size_on_segments(oid) sizes
WHERE relname IN ('heap_table_with_toast', 'heap_table_without_toast',
                  'ao_table_with_toast', 'ao_table_without_toast')
ORDER BY 1, 2;

-- Add random large data to get non-zero toast table's size
UPDATE heap_table_with_toast SET b = (
    SELECT string_agg( chr(trunc(65+random()*26)::integer), '')
    FROM generate_series(1,50000))
WHERE a = 1;

UPDATE ao_table_with_toast SET b = (
    SELECT string_agg( chr(trunc(65+random()*26)::integer), '')
    FROM generate_series(1,50000))
WHERE a = 1;

SELECT relname, sizes.gp_segment_id, sizes.size
FROM pg_class, arenadata_toolkit.adb_relation_storage_size_on_segments(oid) sizes
WHERE relname IN ('heap_table_with_toast', 'ao_table_with_toast')
ORDER BY 1, 2;

-- Check summary size of tables
SELECT relname, adb_relation_storage_size size
FROM pg_class, arenadata_toolkit.adb_relation_storage_size(oid)
WHERE relname IN ('heap_table_with_toast', 'heap_table_without_toast',
                  'ao_table_with_toast', 'ao_table_without_toast')
ORDER BY 1;

-- Cleanup
DROP TABLE heap_table_with_toast;
DROP TABLE heap_table_without_toast;
DROP TABLE ao_table_with_toast;
DROP TABLE ao_table_without_toast;

DROP EXTENSION arenadata_toolkit;
