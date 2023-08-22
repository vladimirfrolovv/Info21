CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE
    peers
(
    nickname_peer VARCHAR PRIMARY KEY,
    birthday_peer DATE NOT NULL
);

CREATE TABLE
    tasks
(
    title       VARCHAR PRIMARY KEY,
    parent_task VARCHAR,
    max_xp      NUMERIC NOT NULL,
    CONSTRAINT fk_tasks_parent_task FOREIGN KEY (parent_task) REFERENCES tasks (title)
);

CREATE TABLE
    checks
(
    id           SERIAL PRIMARY KEY,
    checked_peer VARCHAR NOT NULL,
    task         VARCHAR NOT NULL,
    check_date   DATE    NOT NULL,
    CONSTRAINT fk_checks_task FOREIGN KEY (task) REFERENCES tasks (title),
    CONSTRAINT fk_checks_peer_nickname FOREIGN KEY (checked_peer) REFERENCES peers (nickname_peer)
);

CREATE TABLE
    p2p
(
    id            SERIAL PRIMARY KEY,
    check_id      BIGINT                 NOT NULL,
    checking_peer VARCHAR                NOT NULL,
    state         CHECK_STATUS           NOT NULL,
    time_check    TIME WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT fk_p2p_check_id FOREIGN KEY (check_id) REFERENCES checks (id),
    CONSTRAINT fk_p2p_peer_nickname FOREIGN KEY (checking_peer) REFERENCES peers (nickname_peer)
);

CREATE TABLE
    transferredPoints
(
    id            SERIAL PRIMARY KEY,
    checking_peer VARCHAR NOT NULL,
    checked_peer  VARCHAR NOT NULL,
    pointsAmount  BIGINT  NOT NULL,
    CONSTRAINT fk_transferredPoints_checking_peer FOREIGN KEY (checking_peer) REFERENCES peers (nickname_peer),
    CONSTRAINT fk_transferredPoints_checked_peer FOREIGN KEY (checked_peer) REFERENCES peers (nickname_peer)
);

CREATE TABLE
    verter
(
    id       SERIAL PRIMARY KEY,
    check_id BIGINT                 NOT NULL,
    state    CHECK_STATUS           NOT NULL,
    time     TIME WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT fk_verter_check_id FOREIGN KEY (check_id) REFERENCES checks (id)
);

CREATE TABLE
    friends
(
    id              SERIAL PRIMARY KEY,
    nickname_peer_1 VARCHAR NOT NULL,
    nickname_peer_2 VARCHAR NOT NULL,
    CONSTRAINT fk_friends_nickname_peer_1 FOREIGN KEY (nickname_peer_1) REFERENCES peers (nickname_peer),
    CONSTRAINT fk_friends_nickname_peer_2 FOREIGN KEY (nickname_peer_2) REFERENCES peers (nickname_peer)
);

CREATE TABLE
    recommendations
(
    id                      SERIAL PRIMARY KEY,
    nickname_peer           VARCHAR NOT NULL,
    nickname_recommend_peer VARCHAR NOT NULL,
    CONSTRAINT fk_recommendations_nickname_peer FOREIGN KEY (nickname_peer) REFERENCES peers (nickname_peer),
    CONSTRAINT fk_friends_nickname_recommend_peer FOREIGN KEY (nickname_recommend_peer) REFERENCES peers (nickname_peer)
);

CREATE TABLE
    xp
(
    id        SERIAL PRIMARY KEY,
    check_id  BIGINT  NOT NULL,
    number_xp INTEGER NOT NULL,
    CONSTRAINT fk_xp_check_id FOREIGN KEY (check_id) REFERENCES checks (id)
);

CREATE TABLE
    timeTracking
(
    id            SERIAL PRIMARY KEY,
    nickname_peer VARCHAR                NOT NULL,
    date_track    DATE                   NOT NULL,
    time_track    TIME WITHOUT TIME ZONE NOT NULL,
    state         INTEGER                NOT NULL,
    CONSTRAINT fk_timeTracking_state CHECK (state IN (1, 2)),
    CONSTRAINT fk_timeTracking_nickname_peer FOREIGN KEY (nickname_peer) REFERENCES peers (nickname_peer)
);

CREATE OR REPLACE PROCEDURE PRO_CSV_TO_TABLE(table_ VARCHAR, delimiter VARCHAR) AS
$$
DECLARE
    abs_path VARCHAR = '/Users/garroshm/Proj/SQL2_Info21_v1.0-0/src';
BEGIN
    EXECUTE format(
            'COPY %s FROM ''%s/import/%s.csv'' WITH (FORMAT CSV, DELIMITER ''%s'', HEADER)',
            table_, abs_path, table_, delimiter);
    IF 'id' IN (SELECT column_name FROM information_schema.columns WHERE table_name = LOWER(table_)) THEN
        EXECUTE format(
                'SELECT SETVAL(''%s_id_seq'', MAX(id)) FROM %s',
                LOWER(table_), table_);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE PRO_IMPORT_FILE(delimiter VARCHAR) AS
$$
DECLARE
    tables VARCHAR[] = ARRAY ['peers', 'tasks', 'checks', 'xp', 'friends', 'timeTracking', 'transferredPoints',
        'p2p', 'verter', 'recommendations'];
BEGIN
    FOR i IN 1..ARRAY_LENGTH(tables, 1)
        LOOP
            CALL PRO_CSV_TO_TABLE(tables[i], delimiter);
        END LOOP;
END
$$ LANGUAGE plpgsql;

CALL PRO_IMPORT_FILE(',');



CREATE OR REPLACE PROCEDURE PRO_TABLE_TO_CSV(table_ VARCHAR, delimiter VARCHAR) AS
$$
DECLARE
    abs_path VARCHAR = '/Users/garroshm/Proj/SQL2_Info21_v1.0-0/src';
BEGIN
    EXECUTE format(
            'COPY %s TO ''%s/export/%s.csv'' WITH (FORMAT CSV, DELIMITER ''%s'', HEADER)',
            table_, abs_path, table_, delimiter);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE PRO_EXPORT_FILE(delimiter VARCHAR) AS
$$
DECLARE
    tables VARCHAR[] = ARRAY ['peers', 'tasks', 'checks', 'xp', 'friends', 'timeTracking', 'transferredPoints',
        'p2p', 'verter', 'recommendations'];
BEGIN
    FOR i IN 1..ARRAY_LENGTH(tables, 1)
        LOOP
            CALL PRO_TABLE_TO_CSV(tables[i], delimiter);
        END LOOP;
END
$$ LANGUAGE plpgsql;

CALL PRO_EXPORT_FILE(',');