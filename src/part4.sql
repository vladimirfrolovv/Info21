-- 0)

CREATE TABLE example_1
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE example_2
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE example_3
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE test_1
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE test_2
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE test_3
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE OR REPLACE FUNCTION FNC_INSERT_EXAMPLE() RETURNS TRIGGER AS
$$
BEGIN
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION FNC_UPDATE_EXAMPLE() RETURNS TRIGGER AS
$$
BEGIN
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION FNC_INSERT_TEST() RETURNS TRIGGER AS
$$
BEGIN
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION FNC_UPDATE_TEST() RETURNS TRIGGER AS
$$
BEGIN
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER TRG_EXAMPLE_1
    AFTER INSERT
    ON example_1
    FOR EACH ROW
EXECUTE FUNCTION FNC_INSERT_EXAMPLE();

CREATE TRIGGER TRG_EXAMPLE_2
    BEFORE INSERT
    ON example_2
    FOR EACH ROW
EXECUTE FUNCTION FNC_INSERT_EXAMPLE();

CREATE TRIGGER TRG_EXAMPLE_3
    BEFORE UPDATE
    ON example_3
    FOR EACH ROW
EXECUTE FUNCTION FNC_UPDATE_EXAMPLE();

CREATE TRIGGER TRG_TEST_1
    AFTER INSERT
    ON test_1
    FOR EACH ROW
EXECUTE FUNCTION FNC_INSERT_TEST();

CREATE OR REPLACE FUNCTION FNC_TASK_2_TEST_1(IN a INTEGER, INOUT result INTEGER) AS
$$
BEGIN
    SELECT 1 INTO a;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION FNC_TASK_2_TEST_2(IN a INTEGER, IN b INTEGER, INOUT c INTEGER) AS
$$
BEGIN
    SELECT 1 INTO a;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE PRO_TASK_2_TEST_3(IN a INTEGER, IN b INTEGER, INOUT c INTEGER) AS
$$
BEGIN
    SELECT 1 INTO a;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION FNC_TASK_2_TEST_4(IN a INTEGER) RETURNS INTEGER AS
$$
BEGIN
    SELECT 1 INTO a;
    RETURN 10;
END;
$$ LANGUAGE plpgsql;

-- 1)

CREATE OR REPLACE PROCEDURE PRO_DELETE_TABLE() AS
$$
DECLARE
    table_name_tmp VARCHAR = 'test';
    row            RECORD;
BEGIN
    FOR row IN
        (SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'public'
           AND table_name LIKE format('%s%%', table_name_tmp))
        LOOP
            EXECUTE 'DROP TABLE ' || row.table_name || ' CASCADE';
        END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL PRO_DELETE_TABLE();

-- 2)

CREATE OR REPLACE PROCEDURE PRO_LIST_OF_FUNCTION(INOUT result INTEGER, IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT p.proname,
               pg_catalog.PG_GET_FUNCTION_IDENTITY_ARGUMENTS(p.oid) AS arg
        FROM pg_catalog.pg_namespace n
                 JOIN
             pg_catalog.pg_proc p ON
                 p.pronamespace = n.oid
        WHERE p.prokind = 'f'
          AND pg_catalog.PG_GET_FUNCTION_IDENTITY_ARGUMENTS(p.oid) != ''
          AND n.nspname = 'public';
    result := (SELECT COUNT(p.proname)
               FROM pg_catalog.pg_namespace n
                        JOIN
                    pg_catalog.pg_proc p ON
                        p.pronamespace = n.oid
               WHERE p.prokind = 'f'
                 AND pg_catalog.PG_GET_FUNCTION_IDENTITY_ARGUMENTS(p.oid) != ''
                 AND n.nspname = 'public');
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_LIST_OF_FUNCTION(0, 'ref');
FETCH ALL IN "ref";
END;

-- 3)

CREATE OR REPLACE PROCEDURE PRO_DELETE_TRIGGERS(INOUT count INTEGER) AS
$$
DECLARE
    row RECORD;
BEGIN
    count = (SELECT COUNT(*) FROM information_schema.triggers);
    FOR row IN (SELECT trigger_name, event_object_table FROM information_schema.triggers)
        LOOP
            EXECUTE format('DROP TRIGGER %s ON %s CASCADE', row.trigger_name, row.event_object_table);
        END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL PRO_DELETE_TRIGGERS(NULL);

-- 4)

CREATE OR REPLACE PROCEDURE PRO_FIND_CONTENT(template VARCHAR, ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT proname AS name,
               CASE
                   WHEN prokind = 'p' THEN 'procedure'
                   WHEN prokind = 'f' THEN 'function'
                   END AS type
        FROM pg_proc
                 JOIN pg_namespace p ON
            p.oid = pg_proc.pronamespace AND nspname = 'public'
        WHERE prosrc LIKE '%' || template || '%';
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_FIND_CONTENT('BEGIN', 'ref');
FETCH ALL IN "ref";
END;
