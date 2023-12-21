-- Change log level to disable notice messages from PL/pgSQL and dropped objects
-- from "DROP SCHEMA arenadata_toolkit CASCADE;"
SET client_min_messages=WARNING;

-- We have only first and last versions of arenadata_toolkit and scripts to update
-- it to intermediate versions.
-- At this test we will be use first version and upgrade scripts.
-- Field tablespace_location was added at version 1.4 of arenadata_toolkit
-- Function returns set of successful checks.
CREATE FUNCTION do_upgrade_test_for_arenadata_toolkit(
	from_version TEXT, to_version TEXT)
RETURNS setof TEXT
AS $$
BEGIN

-- Simple check: only create and alter extension:
	CREATE EXTENSION arenadata_toolkit VERSION '1.0';
	IF (from_version != '1.0')
	THEN
		EXECUTE FORMAT($fmt$ALTER EXTENSION arenadata_toolkit
							UPDATE TO %1$I;$fmt$, from_version);
	END IF;
	EXECUTE FORMAT($fmt$ALTER EXTENSION arenadata_toolkit
						UPDATE TO %1$I;$fmt$, to_version);

-- Check the result
	IF to_version = (SELECT extversion
				     FROM pg_extension
				     WHERE extname='arenadata_toolkit')
	THEN
		RETURN NEXT from_version || ': only alter check';
	END IF;

-- Cleanup before next step
	DROP EXTENSION arenadata_toolkit;
	DROP SCHEMA arenadata_toolkit CASCADE;

-- Create, adb_create_tables and alter extension:
	CREATE EXTENSION arenadata_toolkit VERSION '1.0';
	IF (from_version != '1.0')
	THEN
		EXECUTE FORMAT($fmt$ALTER EXTENSION arenadata_toolkit
							UPDATE TO %1$I;$fmt$, from_version);
	END IF;
	PERFORM arenadata_toolkit.adb_create_tables();
	EXECUTE FORMAT($fmt$ALTER EXTENSION arenadata_toolkit
						UPDATE TO %1$I;$fmt$, to_version);

-- Check the result
	IF to_version = (SELECT extversion
				     FROM pg_extension
				     WHERE extname='arenadata_toolkit')
	THEN
		RETURN NEXT from_version || ': alter and create_tables check';
	END IF;

-- Cleanup before next step
	DROP EXTENSION arenadata_toolkit;
	DROP SCHEMA arenadata_toolkit CASCADE;

-- Create, adb_create_tables, adb_collect_table_stats and alter extension:
	CREATE EXTENSION arenadata_toolkit VERSION '1.0';
	IF (from_version != '1.0')
	THEN
		EXECUTE FORMAT($fmt$ALTER EXTENSION arenadata_toolkit
						UPDATE TO %1$I;$fmt$, from_version);
	END IF;
	PERFORM arenadata_toolkit.adb_create_tables();
	PERFORM arenadata_toolkit.adb_collect_table_stats();
	EXECUTE FORMAT($fmt$ALTER EXTENSION arenadata_toolkit
						UPDATE TO %1$I;$fmt$, to_version);

-- Check the result
	IF to_version = (SELECT extversion
				     FROM pg_extension
				     WHERE extname='arenadata_toolkit')
	THEN
		RETURN NEXT from_version || ': alter, create_tables and collect_table_stats check';
	END IF;

-- Check field "tablespace_location" and table "db_files_history_backup_YYYYMMDDtHHMMSS"
-- which were added at version 1.4
	PERFORM arenadata_toolkit.adb_create_tables();

	IF EXISTS (SELECT 1
			   FROM information_schema.columns
			   WHERE table_schema='arenadata_toolkit' AND
			         table_name='db_files_current' AND
			         column_name='tablespace_location')
	THEN
		RETURN NEXT from_version || ': tablespace_location at arenadata_toolkit.db_files_current check';
	END IF;
	IF EXISTS (SELECT 1
			   FROM information_schema.columns
			   WHERE table_schema='arenadata_toolkit' AND
			         table_name='__db_files_current' AND
			         column_name='tablespace_location')
	THEN
		RETURN NEXT from_version || ': tablespace_location at arenadata_toolkit.__db_files_current check';
	END IF;
	IF EXISTS (SELECT 1
				   FROM information_schema.columns
				   WHERE table_schema='arenadata_toolkit' AND
				         table_name='__db_files_current_unmapped' AND
				         column_name='tablespace_location')
	THEN
		RETURN NEXT from_version || ': tablespace_location at arenadata_toolkit.__db_files_current_unmapped check';
	END IF;
	IF EXISTS (SELECT 1
			   FROM information_schema.columns
			   WHERE table_schema='arenadata_toolkit' AND
			         table_name='db_files_history' AND
			         column_name='tablespace_location')
	THEN
		RETURN NEXT from_version || ': tablespace_location at arenadata_toolkit.db_files_history check';
	END IF;
	IF EXISTS (SELECT 1
			   FROM pg_tables
			   WHERE schemaname='arenadata_toolkit' AND
			         tablename like 'db_files_history_backup%')
	THEN
		RETURN NEXT from_version || ': db_files_history_backup check';
	END IF;

-- Cleanup before next step
	DROP EXTENSION arenadata_toolkit;

-- Check create extension with the latest version after current was installed and dropped
	CREATE EXTENSION arenadata_toolkit;
	PERFORM arenadata_toolkit.adb_create_tables();
	PERFORM arenadata_toolkit.adb_collect_table_stats();

-- Check the result
	IF EXISTS (SELECT 1
			   FROM pg_available_extensions
			   WHERE name='arenadata_toolkit' AND
			         default_version=installed_version)
	THEN
		RETURN NEXT from_version || ': create the latest check';
	END IF;

-- Cleanup
	DROP EXTENSION arenadata_toolkit;
	DROP SCHEMA arenadata_toolkit CASCADE;

END$$
LANGUAGE plpgsql;

CREATE FUNCTION do_test_for_each_versions()
RETURNS setof TEXT
AS $$
DECLARE
	version RECORD;
BEGIN
	FOR version IN
		SELECT *
		FROM (VALUES ('1.0'),('1.1'),('1.2'),('1.3'))
			AS from_versions
	LOOP
		RETURN QUERY SELECT do_upgrade_test_for_arenadata_toolkit(
								version.column1::TEXT,
								(SELECT default_version
								 FROM pg_available_extensions
								 WHERE name = 'arenadata_toolkit'))
					 ORDER BY 1;
	END LOOP;
END$$
LANGUAGE plpgsql;

SELECT do_test_for_each_versions() ORDER BY 1;

-- Cleanup
DROP FUNCTION do_test_for_each_versions();
DROP FUNCTION do_upgrade_test_for_arenadata_toolkit(TEXT, TEXT);
RESET client_min_messages;
