-- 1) Write a function that returns the TransferredPoints table in a more human-readable form

CREATE OR REPLACE FUNCTION FNK_GET_TRANSFERRED_POINTS()
    RETURNS TABLE
            (
                peer1        VARCHAR,
                peer2        VARCHAR,
                pointsamount INTEGER
            )
AS
$$
SELECT checking_peer            AS Peer1,
       checked_peer             AS Peer2,
       SUM(result.pointsamount) AS PointsAmount
FROM (SELECT checking_Peer,
             checked_Peer,
             pointsamount
      FROM TransferredPoints
      UNION
      SELECT checked_Peer,
             checking_Peer,
             - pointsamount
      FROM transferredpoints) AS result
WHERE checking_peer < checked_peer
GROUP BY Peer1, Peer2
ORDER BY Peer1
$$ LANGUAGE SQL;

SELECT *
FROM FNK_GET_TRANSFERRED_POINTS();

-- 2) Write a function that returns a table of the following form: user name, name of the checked task,
--    number of XP received

CREATE OR REPLACE FUNCTION FNK_GET_SUCCESS_CHECKS()
    RETURNS TABLE
            (
                peer VARCHAR,
                task VARCHAR,
                xp   BIGINT
            )
AS
$$
SELECT checks.checked_peer, tasks.title, xp.number_xp
FROM checks
         JOIN xp ON xp.check_id = checks.id
         JOIN tasks ON tasks.title = checks.task;
$$ LANGUAGE SQL;

SELECT *
FROM FNK_GET_SUCCESS_CHECKS();

-- 3) Write a function that finds the peers who have not LEFT campus FOR the whole day

CREATE OR REPLACE FUNCTION FNK_GET_NOT_LEFT_PEERS(day_check DATE)
    RETURNS TABLE
            (
                nickname_peer VARCHAR
            )
AS
$$
SELECT nickname_peer
FROM (SELECT nickname_peer, COUNT(state) AS count
      FROM timetracking
      WHERE date_track = day_check
        AND state = 2
      GROUP BY nickname_peer) AS target
WHERE count = 1;
$$ LANGUAGE SQL;

SELECT *
FROM FNK_GET_NOT_LEFT_PEERS('2023-03-03');

-- 4) Find the percentage of successful AND unsuccessful checks FOR all TIME

CREATE OR REPLACE PROCEDURE PRO_GET_PERCENTAGE_SUCCESS_CHECKS(ref REFCURSOR) AS
$$
DECLARE
    successful_checks INTEGER = (SELECT COUNT(checked_peer)
                                 FROM checks
                                          RIGHT JOIN xp ON xp.check_id = checks.id
                                          JOIN tasks ON Tasks.title = checks.task);
    all_checks        INTEGER = (SELECT COUNT(id)
                                 FROM checks);
    prosent_success   INTEGER = (successful_checks::NUMERIC / all_checks::NUMERIC * 100)::INT;

BEGIN
    OPEN ref FOR
        SELECT prosent_success       AS SuccessfulCheck,
               100 - prosent_success AS UnsuccessfulChecks;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_GET_PERCENTAGE_SUCCESS_CHECKS('ref');
FETCH ALL IN "ref";
END;

--5) Calculate the change in the number of peer points of each peer using the TransferredPoints table

CREATE OR REPLACE PROCEDURE PRO_GET_PEER_POINTS_PER_PEER(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        WITH all_transfer AS (SELECT checking_Peer            AS Peer1,
                                     checked_Peer             AS Peer2,
                                     SUM(result.pointsamount) AS PointsAmount
                              FROM (SELECT checking_Peer,
                                           checked_Peer,
                                           pointsamount
                                    FROM TransferredPoints
                                    UNION
                                    SELECT checked_Peer,
                                           checking_Peer,
                                           -pointsamount
                                    FROM transferredpoints) AS result
                              WHERE checking_peer < checked_peer
                              GROUP BY Peer1, Peer2
                              ORDER BY Peer1)
        SELECT peer, SUM(PointsChange) AS PointsChange
        FROM (SELECT peer1 AS peer, SUM(pointsamount) AS PointsChange
              FROM all_transfer
              GROUP BY peer1
              UNION
              SELECT peer2 AS peer, -1 * SUM(pointsamount) AS PointsChange
              FROM all_transfer
              GROUP BY peer2
              ORDER BY PointsChange DESC) AS tmp
        GROUP BY peer
        ORDER BY PointsChange DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_GET_PEER_POINTS_PER_PEER('ref');
FETCH ALL IN "ref";
END;

--6) Calculate the change in the number of peer points of each peer using the table returned by the first function

CREATE OR REPLACE PROCEDURE PRO_GET_PEER_POINTS_PER_PEER_TASK_6(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        SELECT peer, SUM(PointsChange) AS PointsChange
        FROM (SELECT peer1 AS peer, SUM(pointsamount) AS PointsChange
              FROM FNK_GET_TRANSFERRED_POINTS()
              GROUP BY peer1
              UNION
              SELECT peer2 AS peer, -1 * SUM(pointsamount) AS PointsChange
              FROM FNK_GET_TRANSFERRED_POINTS()
              GROUP BY peer2
              ORDER BY PointsChange DESC) AS tmp
        GROUP BY peer
        ORDER BY PointsChange DESC;
END ;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_GET_PEER_POINTS_PER_PEER_TASK_6('ref');
FETCH ALL IN "ref";
END;

-- 7) Find the most frequently checked task FOR each day

CREATE OR REPLACE PROCEDURE PRO_GET_MOST_FREQUENTLY_TASKS_PER_DAY(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        WITH tmp AS (SELECT checks.check_date, tasks.title, COUNT(tasks.title) AS count
                     FROM checks
                              JOIN tasks ON checks.task = tasks.title
                     GROUP BY checks.check_date, tasks.title
                     ORDER BY count DESC, check_date)
        SELECT check_date, title
        FROM (SELECT *, RANK() OVER (PARTITION BY check_date ORDER BY count DESC ) AS rank
              FROM tmp
              GROUP BY check_date, title, count) AS subquery
        WHERE rank = 1
        ORDER BY check_date;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_GET_MOST_FREQUENTLY_TASKS_PER_DAY('ref');
FETCH ALL IN "ref";
END;

--8) Determine the duration of the last P2P check

CREATE OR REPLACE PROCEDURE PRO_LAST_P2P(ref REFCURSOR) AS
$$
DECLARE
    max_id    INTEGER = (SELECT id
                         FROM p2p
                         WHERE state = 'Success'
                            OR state = 'Failure'
                         ORDER BY id DESC
                         LIMIT 1);
    max_check INTEGER = (SELECT check_id
                         FROM p2p
                         WHERE id = max_id);
BEGIN
    OPEN ref FOR
        SELECT ((SELECT time_check FROM p2p WHERE id = max_id) -
                (SELECT time_check FROM p2p WHERE check_id = max_check AND state = 'Start'))::TIME AS check_duration;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_LAST_P2P('ref');
FETCH ALL IN "ref";
END;

-- 9) Find all peers who have completed the whole given block of tasks and the completion date of the last task

CREATE OR REPLACE PROCEDURE PRO_BLOCK_DONE(template VARCHAR, ref REFCURSOR) AS
$$
DECLARE
    _task VARCHAR := (WITH RECURSIVE tmp AS (SELECT 1 AS counter, title, parent_task
                                             FROM tasks
                                             WHERE (parent_task NOT SIMILAR TO FORMAT('%s[[:digit:]]_%%', template) OR
                                                    parent_task IS NULL)
                                               AND title SIMILAR TO FORMAT('%s[[:digit:]]_%%', template)
                                             UNION ALL
                                             SELECT tmp.counter + 1, t.title, t.parent_task
                                             FROM tmp
                                                      JOIN tasks t ON t.parent_task = tmp.title
                                             WHERE t.parent_task = tmp.title
                                               AND t.title SIMILAR TO FORMAT('%s[[:digit:]]_%%', template))
                      SELECT title
                      FROM tmp
                      ORDER BY counter DESC
                      LIMIT 1);
BEGIN
    OPEN ref FOR
        SELECT checked_peer AS Peer, MAX(check_date) AS day
        FROM checks
                 JOIN p2p p ON checks.id = p.check_id
                 LEFT JOIN verter v ON checks.id = v.check_id AND (p.state = v.state)
        WHERE task = _task
          AND p.state = 'Success'
        GROUP BY checked_peer
        ORDER BY day DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_BLOCK_DONE('C', 'ref');
FETCH ALL IN "ref";
END;

-- 10) Determine which peer each student should go to for a check.

CREATE OR REPLACE PROCEDURE PRO_RECOMMENDATION(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        SELECT nickname_peer_1, nickname_recommend_peer
        FROM (WITH union_table AS (SELECT nickname_peer_1, nickname_peer_2
                                   FROM friends
                                   UNION
                                   SELECT nickname_peer_2, nickname_peer_1
                                   FROM friends)
              SELECT nickname_peer_1,
                     nickname_recommend_peer,
                     ROW_NUMBER()
                     OVER (PARTITION BY nickname_peer_1 ORDER BY COUNT(nickname_recommend_peer) DESC) AS row_number
              FROM union_table
                       INNER JOIN recommendations r ON union_table.nickname_peer_2 = r.nickname_peer AND
                                                       union_table.nickname_peer_1 != r.nickname_recommend_peer
              GROUP BY nickname_peer_1, nickname_recommend_peer
              ORDER BY row_number) AS tmp
        WHERE row_number = 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_RECOMMENDATION('ref');
FETCH ALL IN "ref";
END;

-- 11) Determine the percentage of peers

CREATE OR REPLACE PROCEDURE PRO_PERCENT_OF_TWO_BLOCKS(block1 VARCHAR, block2 VARCHAR, ref REFCURSOR) AS
$$
DECLARE
    count_of_peers             INTEGER = (SELECT COUNT(nickname_peer)
                                          FROM peers);
    count_peers_begin_1        INTEGER = (SELECT COUNT(*)
                                          FROM (SELECT checked_peer
                                                FROM checks
                                                WHERE task SIMILAR TO FORMAT('%s[[:digit:]]_%%', block1)
                                                EXCEPT
                                                SELECT checked_peer
                                                FROM checks
                                                WHERE task SIMILAR TO FORMAT('%s[[:digit:]]_%%', block2))
                                                   AS tmp_block_1);
    count_peers_begin_2        INTEGER = (SELECT COUNT(*)
                                          FROM (SELECT checked_peer
                                                FROM checks
                                                WHERE task SIMILAR TO FORMAT('%s[[:digit:]]_%%', block2)
                                                EXCEPT
                                                SELECT checked_peer
                                                FROM checks
                                                WHERE task SIMILAR TO FORMAT('%s[[:digit:]]_%%', block1))
                                                   AS tmp_block_2);
    count_peers_begin_both     INTEGER = (SELECT COUNT(checked_peer)
                                          FROM (SELECT checked_peer
                                                FROM checks
                                                WHERE task SIMILAR TO FORMAT('%s[[:digit:]]_%%', block1)
                                                INTERSECT
                                                SELECT checked_peer
                                                FROM checks
                                                WHERE task SIMILAR TO FORMAT('%s[[:digit:]]_%%', block2)) AS tmp);
    count_peers_not_begin_both INTEGER = (SELECT COUNT(nickname_peer)
                                          FROM (SELECT nickname_peer
                                                FROM peers
                                                EXCEPT
                                                SELECT checked_peer
                                                FROM checks
                                                WHERE task SIMILAR TO FORMAT('%s[[:digit:]]_%%', block1)
                                                EXCEPT
                                                SELECT checked_peer
                                                FROM checks
                                                WHERE task SIMILAR TO FORMAT('%s[[:digit:]]_%%', block2)) AS tmp);
BEGIN
    OPEN ref FOR
        SELECT ((count_peers_begin_1::NUMERIC / count_of_peers::NUMERIC) * 100)::INTEGER        AS StartedBlock1,
               ((count_peers_begin_2::NUMERIC / count_of_peers::NUMERIC) * 100)::INTEGER        AS StartedBlock2,
               ((count_peers_begin_both::NUMERIC / count_of_peers::NUMERIC) * 100)::INTEGER     AS StartedBothBlocks,
               ((count_peers_not_begin_both::NUMERIC / count_of_peers::NUMERIC) * 100)::INTEGER AS DidntStartAnyBlock;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_PERCENT_OF_TWO_BLOCKS('A', 'D', 'ref');
FETCH ALL IN "ref";
END;

--12) Determine N peers WITH the greatest number of friends

CREATE OR REPLACE PROCEDURE PRO_MAX_FRIEND(N INTEGER, ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        SELECT nickname_peer_1 AS peer, COUNT(nickname_peer_2) AS FriendsCount
        FROM ((SELECT id, nickname_peer_1, nickname_peer_2 FROM friends)
              UNION ALL
              (SELECT id, nickname_peer_2, nickname_peer_1 FROM friends)) AS tmp
        GROUP BY nickname_peer_1
        ORDER BY 2 DESC
        LIMIT N;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_MAX_FRIEND(3, 'ref');
FETCH ALL IN "ref";
END;

--13) Determine the percentage of peers who have ever successfully passed a check ON their birthday

CREATE OR REPLACE PROCEDURE PRO_BIRTHDAY_SUCCESS(ref REFCURSOR) AS
$$
DECLARE
    successful_checks INTEGER = (SELECT COUNT(checks.id)
                                 FROM checks
                                          RIGHT JOIN xp ON xp.check_id = checks.id
                                          INNER JOIN (SELECT nickname_peer,
                                                             TO_CHAR(birthday_peer, 'Month') AS bth_month,
                                                             EXTRACT(day FROM birthday_peer) AS bth_day
                                                      FROM peers) AS tmp
                                                     ON tmp.bth_day = EXTRACT(day FROM checks.check_date) AND
                                                        tmp.bth_month = TO_CHAR(checks.check_date, 'Month') AND
                                                        tmp.nickname_peer = checks.checked_peer);
    all_checks        INTEGER = (SELECT COUNT(checks.id)
                                 FROM checks
                                          INNER JOIN (SELECT nickname_peer,
                                                             TO_CHAR(birthday_peer, 'Month') AS bth_month,
                                                             EXTRACT(day FROM birthday_peer) AS bth_day
                                                      FROM peers) AS tmp
                                                     ON tmp.bth_day = EXTRACT(day FROM checks.check_date) AND
                                                        tmp.bth_month = TO_CHAR(checks.check_date, 'Month') AND
                                                        tmp.nickname_peer = checks.checked_peer);
    percent_success   INTEGER = (successful_checks::NUMERIC / all_checks::NUMERIC * 100)::INT;
BEGIN
    OPEN ref FOR
        SELECT percent_success       AS SuccessfulCheck,
               100 - percent_success AS UnsuccessfulChecks;
END ;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_BIRTHDAY_SUCCESS('ref');
FETCH ALL IN "ref";
END;

--14) Determine the total amount of XP gained by each peer

CREATE OR REPLACE PROCEDURE PRO_TOTAL_XP(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        SELECT tmp.checked_peer AS peer, SUM(xp_max) AS XP
        FROM (SELECT checked_peer, MAX(number_xp) AS xp_max
              FROM checks
                       LEFT JOIN xp ON checks.id = xp.check_id
              GROUP BY checked_peer, task) AS tmp
        GROUP BY checked_peer
        ORDER BY xp DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_TOTAL_XP('ref');
FETCH ALL IN "ref";
END;

-- 15) Determine all peers who did the given tasks 1 AND 2, but did not do task 3

CREATE OR REPLACE PROCEDURE PRO_TWO_DONE_BUT_THIRD(ref REFCURSOR, first_task VARCHAR, second_task VARCHAR,
                                                   third_task VARCHAR) AS
$$
BEGIN
    OPEN ref FOR
        SELECT checked_peer
        FROM checks
                 RIGHT JOIN xp ON xp.check_id = checks.id
        WHERE checks.task = first_task
        INTERSECT
        SELECT checked_peer
        FROM checks
                 RIGHT JOIN xp ON xp.check_id = checks.id
        WHERE checks.task = second_task
        EXCEPT
        SELECT checked_peer
        FROM checks
                 LEFT JOIN xp ON xp.check_id = checks.id
        WHERE checks.task = third_task
          AND xp.id IS NULL;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_TWO_DONE_BUT_THIRD('ref', 'SQL1_Bootcamp', 'D1_Linux', 'APP1_PythonBootcamp');
FETCH ALL IN "ref";
END;

-- 16) Using recursive common table expression, output the number of preceding tasks for each task

CREATE OR REPLACE PROCEDURE PRO_COUNT_OF_PREV_TASKS(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        WITH RECURSIVE tmp AS (SELECT title,
                                      title    AS title_buf,
                                      parent_task,
                                      (CASE
                                           WHEN parent_task IS NULL THEN 0
                                           ELSE 1
                                          END) AS count
                               FROM tasks
                               UNION
                               SELECT tmp.title,
                                      tmp.parent_task,
                                      tasks.parent_task,
                                      (CASE
                                           WHEN tasks.parent_task IS NULL THEN tmp.count
                                           ELSE tmp.count + 1
                                          END) AS count
                               FROM tmp
                                        JOIN tasks ON tmp.parent_task = tasks.title
                               WHERE tasks.parent_task IS NOT NULL)
        SELECT title      AS Task,
               MAX(count) AS PrevCount
        FROM tmp
        GROUP BY Task
        ORDER BY Task;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_COUNT_OF_PREV_TASKS('ref');
FETCH ALL IN "ref";
END;

-- 17) Find "lucky" days for checks. A day is considered "lucky" if it has at least N consecutive successful checks

CREATE OR REPLACE PROCEDURE PRO_LUCKY_DAY(N INTEGER, ref REFCURSOR) AS
$$
DECLARE
    line          RECORD;
    queue         INTEGER = 0;
    max_queue     INTEGER = 0;
    previous_date DATE    = (SELECT MIN(check_date)
                             FROM checks);
    dates         DATE[];
    i             INTEGER = 0;
BEGIN
    FOR line IN
        SELECT check_date, number_xp, max_xp
        FROM checks
                 JOIN p2p p ON checks.id = p.check_id
                 LEFT JOIN verter v ON checks.id = v.check_id AND v.state != 'Start'
                 LEFT JOIN xp x ON checks.id = x.check_id
                 LEFT JOIN tasks t ON t.title = checks.task
        WHERE p.state != 'Start'
        ORDER BY check_date, time_check
        LOOP
            IF line.check_date != previous_date THEN
                IF max_queue >= N THEN
                    dates[i] = previous_date;
                    i = i + 1;
                END IF;
                queue = 0;
                max_queue = 0;
                previous_date = line.check_date;
            END IF;
            IF line.number_xp IS NOT NULL THEN
                IF line.number_xp::NUMERIC / line.max_xp::NUMERIC >= 0.8 THEN
                    queue = queue + 1;
                    IF queue > max_queue THEN
                        max_queue = queue;
                    END IF;
                END IF;
            ELSE
                queue = 0;
            END IF;
        END LOOP;
    OPEN ref FOR
        SELECT UNNEST(dates);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_LUCKY_DAY(4, 'ref');
FETCH ALL IN "ref";
END;

--18) Determine the peer with the greatest number of completed tasks

CREATE OR REPLACE PROCEDURE PRO_COUNT_OF_DONE_PROJECTS(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        SELECT checked_peer AS peer, COUNT(xp_max) AS xp
        FROM (SELECT checked_peer, MAX(number_xp) AS xp_max
              FROM checks
                       LEFT JOIN xp ON checks.id = xp.check_id
              GROUP BY checked_peer, task) AS tmp
        GROUP BY checked_peer
        ORDER BY xp DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_COUNT_OF_DONE_PROJECTS('ref');
FETCH ALL IN "ref";
END;

-- 19) Find the peer with the highest amount of XP

CREATE OR REPLACE PROCEDURE PRO_MAX_EXP_PEER(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        SELECT checked_peer AS peer, SUM(xp_max) AS xp
        FROM (SELECT checked_peer, MAX(number_xp) AS xp_max
              FROM checks
                       LEFT JOIN xp ON checks.id = xp.check_id
              GROUP BY checked_peer, task) AS tmp
        GROUP BY checked_peer
        HAVING SUM(xp_max) IS NOT NULL
        ORDER BY xp DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_MAX_EXP_PEER('ref');
FETCH ALL IN "ref";
END;

-- 20) Find the peer who spent the longest amount of time on campus today

CREATE OR REPLACE PROCEDURE PRO_MAX_TIME_IN_DAY_PEER(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        WITH entry_perers AS (SELECT nickname_peer,
                                     time_track,
                                     ROW_NUMBER() OVER (ORDER BY nickname_peer, date_track, time_track) AS row_num
                              FROM timetracking
                              WHERE state = 1
                                AND date_track = current_date),
             exit_peers AS (SELECT nickname_peer,
                                   time_track,
                                   ROW_NUMBER() OVER (ORDER BY nickname_peer, date_track, time_track) AS row_num
                            FROM timetracking
                            WHERE state = 2
                              AND date_track = current_date),
             time_in_campus AS (SELECT entry_perers.nickname_peer,
                                       (exit_peers.time_track - entry_perers.time_track)::TIME AS tmp_time
                                FROM entry_perers
                                         JOIN exit_peers ON entry_perers.row_num = exit_peers.row_num)
        SELECT nickname_peer
        FROM (SELECT nickname_peer, SUM(tmp_time)::TIME AS sum_time
              FROM time_in_campus
              GROUP BY nickname_peer
              ORDER BY sum_time DESC) AS res
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_MAX_TIME_IN_DAY_PEER('ref');
FETCH ALL IN "ref";
END;

--21) Determine the peers that came before the given time at least N times during the whole time

CREATE OR REPLACE PROCEDURE PRO_ENTER_PEER_IN(ref REFCURSOR, time_enter TIME, count_n INTEGER) AS
$$
BEGIN
    OPEN ref FOR
        SELECT nickname_peer
        FROM timetracking
        WHERE time_track < time_enter
          AND state = 1
        GROUP BY nickname_peer
        HAVING COUNT(time_track) >= count_n;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_ENTER_PEER_IN('ref', '9:10', 1);
FETCH ALL IN "ref";
END;

--22) Determine the peers who left the campus more than M times during the last N days

CREATE OR REPLACE PROCEDURE PRO_EXIT_PEER(ref REFCURSOR, count_day INTEGER, count_n INTEGER) AS
$$
BEGIN
    OPEN ref FOR
        SELECT nickname_peer
        FROM timetracking
        WHERE date_track IN (SELECT DISTINCT date_track
                             FROM timetracking
                             WHERE date_track BETWEEN current_date - count_day AND current_date)
          AND state = 2
        GROUP BY nickname_peer
        HAVING COUNT(time_track) > count_n;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_EXIT_PEER('ref', 1, 0);
FETCH ALL IN "ref";
END;

--23) Determine which peer was the last to come in today

CREATE OR REPLACE PROCEDURE PRO_TODAY_LAST_ENTRY(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        SELECT nickname_peer
        FROM timetracking
        WHERE date_track = CURRENT_DATE
          AND state = 1
        ORDER BY time_track DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_TODAY_LAST_ENTRY('ref');
FETCH ALL IN "ref";
END;

-- 24) Determine the peer that left campus yesterday for more than N minutes

CREATE OR REPLACE PROCEDURE PRO_PEERS_WITH_BREAK_YESTERDAY_ON_N_MIN(ref REFCURSOR, break_time INTEGER) AS
$$
BEGIN
    OPEN ref FOR
        SELECT nickname_peer
        FROM (WITH been_t AS (SELECT nickname_peer, (MAX(time_track) - MIN(time_track))::TIME AS been_today
                              FROM timetracking
                              WHERE date_track = current_date - 1
                              GROUP BY nickname_peer),
                   work_today AS (WITH entry_perers AS (SELECT nickname_peer,
                                                               time_track,
                                                               ROW_NUMBER() OVER (ORDER BY nickname_peer, date_track, time_track) AS row_num
                                                        FROM timetracking
                                                        WHERE state = 1
                                                          AND date_track = current_date - 1),
                                       exit_peers AS (SELECT nickname_peer,
                                                             time_track,
                                                             ROW_NUMBER() OVER (ORDER BY nickname_peer, date_track, time_track) AS row_num
                                                      FROM timetracking
                                                      WHERE state = 2
                                                        AND date_track = current_date - 1),
                                       time_in_campus AS (SELECT entry_perers.nickname_peer,
                                                                 (exit_peers.time_track - entry_perers.time_track)::TIME AS tmp_time
                                                          FROM entry_perers
                                                                   JOIN exit_peers ON entry_perers.row_num = exit_peers.row_num)
                                  SELECT nickname_peer, SUM(tmp_time)::TIME AS sum_time
                                  FROM time_in_campus
                                  GROUP BY nickname_peer
                                  ORDER BY sum_time DESC)
              SELECT been_t.nickname_peer, (been_today - work_today.sum_time)::TIME AS break_today
              FROM been_t
                       JOIN work_today ON been_t.nickname_peer = work_today.nickname_peer) AS tmp
        WHERE break_today > CAST(TO_CHAR(break_time, 'FM99909:99') AS
            TIME WITHOUT TIME ZONE);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_PEERS_WITH_BREAK_YESTERDAY_ON_N_MIN('ref', 2000);
FETCH ALL IN "ref";
END;

--25) Determine for each month the percentage of early entries

CREATE OR REPLACE PROCEDURE PRO_RELATION_ENTER(ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        WITH gen AS (SELECT TO_CHAR(gener, 'Month') AS months, 0 AS EarlyEntries
                     FROM GENERATE_SERIES
                              ('2007-01-01'::TIMESTAMP,
                               '2007-12-01'::TIMESTAMP,
                               '1 month'::INTERVAL) AS gener),
             entry_birt AS (SELECT TO_CHAR(date_track, 'Month') AS months, COUNT(*)::NUMERIC AS cout_all
                            FROM timetracking
                                     INNER JOIN peers ON peers.nickname_peer = timetracking.nickname_peer AND
                                                         EXTRACT(month FROM date_track) =
                                                         EXTRACT(month FROM birthday_peer)
                            WHERE state = 1
                            GROUP BY months),
             entry_birt_12 AS (SELECT TO_CHAR(date_track, 'Month') AS months, COUNT(*)::NUMERIC AS count_12
                               FROM timetracking
                                        INNER JOIN peers ON peers.nickname_peer = timetracking.nickname_peer AND
                                                            EXTRACT(month FROM date_track) =
                                                            EXTRACT(month FROM birthday_peer)
                               WHERE state = 1
                                 AND time_track < '12:00:00'
                               GROUP BY months)
        SELECT gen.months                                      AS Month,
               (CASE
                    WHEN (count_12 / cout_all * 100)::INT IS NULL THEN 0
                    ELSE (count_12 / cout_all * 100)::INT END) AS EarlyEntries
        FROM gen
                 LEFT JOIN entry_birt ON entry_birt.months = gen.months
                 LEFT JOIN entry_birt_12 ON entry_birt_12.months = gen.months;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PRO_RELATION_ENTER('ref');
FETCH ALL IN "ref";
END;
