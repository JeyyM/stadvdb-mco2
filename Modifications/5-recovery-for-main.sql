-- RECOVERY FOR MAIN (Run on NODE A or NODE B)
-- Node A/B reads Main's federated transaction_log and pushes missing transactions to Main

CREATE TABLE IF NOT EXISTS recovery_checkpoint (
    node_name VARCHAR(50) PRIMARY KEY,
    last_recovery_timestamp TIMESTAMP(6) NOT NULL,
    recovery_count INT UNSIGNED DEFAULT 0,
    last_transaction_id VARCHAR(50),
    updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

-- Initialize checkpoint for Main
INSERT INTO recovery_checkpoint (node_name, last_recovery_timestamp, recovery_count)
VALUES ('main', '2000-01-01 00:00:00.000000', 0)
ON DUPLICATE KEY UPDATE node_name = node_name;

SELECT 'Recovery checkpoint table ready for Main recovery' AS status;

DROP PROCEDURE IF EXISTS find_missing_on_main;

DELIMITER $$

CREATE PROCEDURE find_missing_on_main(
    IN since_timestamp TIMESTAMP(6)
)
BEGIN
    DECLARE checkpoint_time TIMESTAMP(6);
    
    -- Get last checkpoint time
    SELECT GREATEST(IFNULL(last_recovery_timestamp, '2000-01-01'), since_timestamp)
    INTO checkpoint_time
    FROM recovery_checkpoint
    WHERE node_name = 'main';
    
    -- Find committed transactions in Main's log that we can see via federation
    SELECT 
        tm.transaction_id,
        tm.timestamp,
        tm.operation_type,
        tm.record_id,
        tm.new_value,
        tm.old_value,
        tm.table_name
    FROM transaction_log_main tm
    WHERE tm.log_type = 'COMMIT'
      AND tm.timestamp > checkpoint_time
      AND tm.operation_type IS NOT NULL
    ORDER BY tm.timestamp ASC;
END$$

DELIMITER ;


DROP PROCEDURE IF EXISTS replay_to_main;

DELIMITER $$

CREATE PROCEDURE replay_to_main(
    IN operation_type_param VARCHAR(10),
    IN new_value_json TEXT,
    IN old_value_json TEXT
)
BEGIN
    DECLARE v_tconst VARCHAR(12);
    DECLARE v_title VARCHAR(1024);
    DECLARE v_runtime SMALLINT UNSIGNED;
    DECLARE v_rating DECIMAL(3,1);
    DECLARE v_votes INT UNSIGNED;
    DECLARE v_weighted DECIMAL(4,2);
    DECLARE v_year SMALLINT UNSIGNED;
    
    IF operation_type_param = 'INSERT' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.tconst'));
        SET v_title = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.primaryTitle'));
        SET v_runtime = JSON_EXTRACT(new_value_json, '$.runtimeMinutes');
        SET v_rating = JSON_EXTRACT(new_value_json, '$.averageRating');
        SET v_votes = JSON_EXTRACT(new_value_json, '$.numVotes');
        SET v_weighted = JSON_EXTRACT(new_value_json, '$.weightedRating');
        SET v_year = JSON_EXTRACT(new_value_json, '$.startYear');
        
        -- Insert to local table (will be partition-routed on Main)
        IF NOT EXISTS (SELECT 1 FROM title_ft WHERE tconst = v_tconst) THEN
            INSERT INTO title_ft (tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear)
            VALUES (v_tconst, v_title, v_runtime, v_rating, v_votes, v_weighted, v_year);
            
            SELECT CONCAT('Replayed INSERT to Main: ', v_tconst) AS result;
        ELSE
            SELECT CONCAT('Record already exists on Main: ', v_tconst, ' - Skipped') AS result;
        END IF;
        
    ELSEIF operation_type_param = 'UPDATE' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.tconst'));
        SET v_title = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.primaryTitle'));
        SET v_runtime = JSON_EXTRACT(new_value_json, '$.runtimeMinutes');
        SET v_rating = JSON_EXTRACT(new_value_json, '$.averageRating');
        SET v_votes = JSON_EXTRACT(new_value_json, '$.numVotes');
        SET v_weighted = JSON_EXTRACT(new_value_json, '$.weightedRating');
        SET v_year = JSON_EXTRACT(new_value_json, '$.startYear');
        
        UPDATE title_ft 
        SET primaryTitle = v_title,
            runtimeMinutes = v_runtime,
            averageRating = v_rating,
            numVotes = v_votes,
            weightedRating = v_weighted,
            startYear = v_year
        WHERE tconst = v_tconst;
        
        SELECT CONCAT('Replayed UPDATE to Main: ', v_tconst) AS result;
        
    ELSEIF operation_type_param = 'DELETE' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(old_value_json, '$.tconst'));
        DELETE FROM title_ft WHERE tconst = v_tconst;
        SELECT CONCAT('Replayed DELETE to Main: ', v_tconst) AS result;
        
    ELSE
        SELECT 'Unknown operation type' AS result;
    END IF;
END$$

DELIMITER ;

DROP PROCEDURE IF EXISTS full_recovery_main;

DELIMITER $$

CREATE PROCEDURE full_recovery_main(
    IN since_timestamp TIMESTAMP(6)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_operation VARCHAR(10);
    DECLARE v_new_value TEXT;
    DECLARE v_old_value TEXT;
    DECLARE v_tconst VARCHAR(12);
    DECLARE v_transaction_id VARCHAR(50);
    DECLARE v_timestamp TIMESTAMP(6);
    DECLARE v_max_timestamp TIMESTAMP(6);
    DECLARE recovery_count INT DEFAULT 0;
    DECLARE checkpoint_time TIMESTAMP(6);
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SELECT 'Recovery failed - transaction rolled back' AS result;
    END;
    
    -- Get last checkpoint
    SELECT GREATEST(IFNULL(last_recovery_timestamp, '2000-01-01'), since_timestamp)
    INTO checkpoint_time
    FROM recovery_checkpoint
    WHERE node_name = 'main';
    
    SELECT CONCAT('Starting recovery for Main from checkpoint: ', checkpoint_time) AS status;
    
    START TRANSACTION;
    
    SET v_max_timestamp = checkpoint_time;
    
    BEGIN
        DECLARE cur CURSOR FOR
            SELECT 
                tm.operation_type,
                tm.new_value,
                tm.old_value,
                tm.record_id,
                tm.transaction_id,
                tm.timestamp
            FROM transaction_log_main tm
            WHERE tm.log_type = 'COMMIT'
              AND tm.timestamp > checkpoint_time
              AND tm.operation_type IS NOT NULL
            ORDER BY tm.timestamp ASC;
        
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        
        OPEN cur;
        
        read_loop: LOOP
            FETCH cur INTO v_operation, v_new_value, v_old_value, v_tconst, v_transaction_id, v_timestamp;
            
            IF done THEN
                LEAVE read_loop;
            END IF;
            
            CALL replay_to_main(v_operation, v_new_value, v_old_value);
            SET recovery_count = recovery_count + 1;
            SET v_max_timestamp = v_timestamp;
            
        END LOOP;
        
        CLOSE cur;
    END;
    
    -- Update checkpoint
    UPDATE recovery_checkpoint
    SET last_recovery_timestamp = v_max_timestamp,
        recovery_count = recovery_count + recovery_count,
        last_transaction_id = v_transaction_id
    WHERE node_name = 'main';
    
    COMMIT;
    
    SELECT CONCAT('Recovery complete! Replayed ', recovery_count, ' transactions to Main. Checkpoint saved at: ', v_max_timestamp) AS result;
END$$

DELIMITER ;

SELECT '=== Node A/B → Main Recovery Procedures Created ===' AS status;
SELECT 'Run CALL full_recovery_main(NOW() - INTERVAL 24 HOUR) to recover Main from this node perspective' AS info;
