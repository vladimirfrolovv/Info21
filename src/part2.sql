CREATE OR REPLACE
    PROCEDURE PRO_INSERT_DATA_P2P(checkedPeer VARCHAR, checkingPeer VARCHAR, _task VARCHAR,
                                  check_status CHECK_STATUS, time_check TIME WITHOUT TIME ZONE) AS
$$
DECLARE
    chk_id BIGINT;
    count  INTEGER;
BEGIN
    IF check_status = 'Start' THEN
        INSERT INTO Checks (checked_peer, task, check_date)
        VALUES (checkedPeer, _task, NOW()::DATE);
        chk_id = (SELECT MAX(id) FROM Checks);
    ELSE
        chk_id = (SELECT P2P.check_id
                  FROM P2P
                           JOIN checks c ON c.id = p2p.check_id
                  WHERE checking_peer = checkingPeer
                    AND c.task = _task
                    AND c.checked_peer = checkedPeer
                    AND state = 'Start');
        count = (SELECT COUNT(check_id)
                 FROM P2P
                 WHERE check_id = chk_id);
    END IF;
    IF chk_id IS NULL THEN
        RAISE NOTICE 'Does not exist `Start` state for this check';
    ELSEIF count >= 2 THEN
        RAISE NOTICE 'State Failure\Success already exist';
    ELSE
        INSERT INTO P2P (check_id, checking_peer, state, time_check)
        VALUES (chk_id, checkingPeer, check_status, time_check);
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE
    PROCEDURE PRO_INSERT_DATA_VERTER(_checking_peer VARCHAR, _task VARCHAR, _check_status_vert CHECK_STATUS,
                                     _time_check TIME WITHOUT TIME ZONE) AS
$$
DECLARE
    _check_id BIGINT;
BEGIN
    _check_id = (SELECT c.id
                 FROM P2P
                          JOIN checks c ON c.id = p2p.check_id
                 WHERE checking_peer = _checking_peer
                   AND task = _task
                   AND state = 'Success'
                 ORDER BY check_date DESC, time_check DESC
                 LIMIT 1);
    IF _check_id IS NULL THEN
        RAISE NOTICE 'Does not exist `Success` state for this p2p check';
    ELSE
        INSERT INTO verter (check_id, state, time)
        VALUES (_check_id, _check_status_vert, _time_check::TIME);
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION FNC_CHANGE_TRANSFERREDPOINTS() RETURNS TRIGGER AS
$trg_change_transferredPoints$
DECLARE
    _checking_peer VARCHAR;
    _checked_peer  VARCHAR;
BEGIN
    IF (NEW.state = 'Start') THEN
        _checking_peer = NEW.checking_peer;
        _checked_peer = (SELECT checked_peer
                         FROM Checks
                         WHERE id = NEW.check_id);
        UPDATE TransferredPoints
        SET pointsAmount = pointsAmount + 1
        WHERE checking_peer = _checking_peer
          AND checked_peer = _checked_peer;
    END IF;
    RETURN NEW;
END;
$trg_change_transferredPoints$ LANGUAGE plpgsql;

CREATE TRIGGER trg_change_transferredPoints
    BEFORE INSERT
    ON P2P
    FOR EACH ROW
EXECUTE FUNCTION FNC_CHANGE_TRANSFERREDPOINTS();


CREATE OR REPLACE FUNCTION FNC_CHECK_XP() RETURNS TRIGGER AS
$trg_check_XP$
DECLARE
    max_xp        INTEGER      = (SELECT max_xp
                                  FROM tasks
                                           JOIN checks c ON tasks.title = c.task
                                  WHERE c.id = NEW.check_id);
    status_p2p    CHECK_STATUS = (SELECT state
                                  FROM p2p
                                  WHERE state != 'Start'
                                    AND p2p.check_id = NEW.check_id);
    status_verter CHECK_STATUS = (SELECT state
                                  FROM verter
                                  WHERE (state != 'Start' OR state IS NULL)
                                    AND verter.check_id = NEW.check_id);
BEGIN
    IF (status_p2p = 'Success' AND status_verter = 'Failure') OR (status_p2p != 'Success') THEN
        RAISE NOTICE 'Does not exist `Success` state for this check';
        RETURN NULL;
    END IF;
    IF NEW.number_xp <= max_xp THEN
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$trg_check_XP$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_XP
    BEFORE INSERT
    ON XP
    FOR EACH ROW
EXECUTE FUNCTION FNC_CHECK_XP();
