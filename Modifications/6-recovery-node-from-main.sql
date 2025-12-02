-- ============================================================================
-- RECOVERY: Use Main's Logs to Recover Node A or Node B
-- Case #4: Node A/B recovers from failure and missed transactions
-- WITH CHECKPOINT SYSTEM to prevent duplicate replay
-- ============================================================================

USE `stadvdb-mco2`;

-- ============================================================================
-- 0. CREATE RECOVERY CHECKPOINT TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS recovery_checkpoint (
    node_name VARCHAR(50) PRIMARY KEY,
    last_recovery_timestamp TIMESTAMP(6) NOT NULL,
    recovery_count INT UNSIGNED DEFAULT 0,
    last_transaction_id VARCHAR(50),
    updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

-- Initialize checkpoint entries
INSERT INTO recovery_checkpoint (node_name, last_recovery_timestamp, recovery_count)
VALUES 
    ('node_a', '2000-01-01 00:00:00.000000', 0),
    ('node_b', '2000-01-01 00:00:00.000000', 0)
ON DUPLICATE KEY UPDATE node_name = node_name;

SELECT '✅ Recovery checkpoint table created' AS status;

-- ============================================================================
-- 1. FIND MISSING TRANSACTIONS ON NODE A (compared to Main's log)
-- ============================================================================
DROP PROCEDURE IF EXISTS find_missing_on_node_a;

DELIMITER $$

CREATE PROCEDURE find_missing_on_node_a(
    IN since_timestamp TIMESTAMP(6)
)
BEGIN
    DECLARE checkpoint_time TIMESTAMP(6);
    
    -- Get last checkpoint time (use the LATER of checkpoint vs provided timestamp)
    SELECT GREATEST(last_recovery_timestamp, since_timestamp)
    INTO checkpoint_time
    FROM recovery_checkpoint
    WHERE node_name = 'node_a';
    
    -- Find committed transactions in Main's log that should have gone to Node A
    -- but are AFTER the last checkpoint
    SELECT 
        tm.transaction_id,
        tm.timestamp,
        tm.operation_type,
        tm.record_id,
        tm.new_value,
        tm.old_value,
        tm.table_name
    FROM transaction_log tm
    WHERE tm.log_type = 'COMMIT'
      AND tm.timestamp > checkpoint_time  -- Only transactions AFTER last checkpoint
      AND tm.operation_type IS NOT NULL
      -- Check if this record should be on Node A (startYear >= 2025)
      AND (
          (tm.operation_type = 'INSERT' AND JSON_EXTRACT(tm.new_value, '$.startYear') >= 2025)
          OR (tm.operation_type = 'UPDATE' AND JSON_EXTRACT(tm.new_value, '$.startYear') >= 2025)
          OR (tm.operation_type = 'DELETE' AND JSON_EXTRACT(tm.old_value, '$.startYear') >= 2025)
      )
    ORDER BY tm.timestamp ASC;
END$$

DELIMITER ;

-- ============================================================================
-- 2. FIND MISSING TRANSACTIONS ON NODE B (compared to Main's log)
-- ============================================================================
DROP PROCEDURE IF EXISTS find_missing_on_node_b;

DELIMITER $$

CREATE PROCEDURE find_missing_on_node_b(
    IN since_timestamp TIMESTAMP(6)
)
BEGIN
    DECLARE checkpoint_time TIMESTAMP(6);
    
    -- Get last checkpoint time (use the LATER of checkpoint vs provided timestamp)
    SELECT GREATEST(last_recovery_timestamp, since_timestamp)
    INTO checkpoint_time
    FROM recovery_checkpoint
    WHERE node_name = 'node_b';
    
    -- Find committed transactions in Main's log that should have gone to Node B
    -- but are AFTER the last checkpoint
    SELECT 
        tm.transaction_id,
        tm.timestamp,
        tm.operation_type,
        tm.record_id,
        tm.new_value,
        tm.old_value,
        tm.table_name
    FROM transaction_log tm
    WHERE tm.log_type = 'COMMIT'
      AND tm.timestamp > checkpoint_time  -- Only transactions AFTER last checkpoint
      AND tm.operation_type IS NOT NULL
      -- Check if this record should be on Node B (startYear < 2025 or NULL)
      AND (
          (tm.operation_type = 'INSERT' AND (JSON_EXTRACT(tm.new_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.new_value, '$.startYear') IS NULL))
          OR (tm.operation_type = 'UPDATE' AND (JSON_EXTRACT(tm.new_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.new_value, '$.startYear') IS NULL))
          OR (tm.operation_type = 'DELETE' AND (JSON_EXTRACT(tm.old_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.old_value, '$.startYear') IS NULL))
      )
    ORDER BY tm.timestamp ASC;
END$$

DELIMITER ;

-- ============================================================================
-- 3. REPLAY TRANSACTION TO NODE A (Direct Insert)
-- ============================================================================
DROP PROCEDURE IF EXISTS replay_to_node_a;

DELIMITER $$

CREATE PROCEDURE replay_to_node_a(
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
        -- Extract values from JSON
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.tconst'));
        SET v_title = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.primaryTitle'));
        SET v_runtime = JSON_EXTRACT(new_value_json, '$.runtimeMinutes');
        SET v_rating = JSON_EXTRACT(new_value_json, '$.averageRating');
        SET v_votes = JSON_EXTRACT(new_value_json, '$.numVotes');
        SET v_weighted = JSON_EXTRACT(new_value_json, '$.weightedRating');
        SET v_year = JSON_EXTRACT(new_value_json, '$.startYear');
        
        -- Check if record already exists on Node A (via federated table)
        IF NOT EXISTS (SELECT 1 FROM title_ft_node_a WHERE tconst = v_tconst) THEN
            -- Direct insert to Node A via federated table
            INSERT INTO title_ft_node_a (tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear)
            VALUES (v_tconst, v_title, v_runtime, v_rating, v_votes, v_weighted, v_year);
            
            SELECT CONCAT('✅ Replayed INSERT to Node A: ', v_tconst) AS result;
        ELSE
            SELECT CONCAT('⚠️ Record already exists on Node A: ', v_tconst, ' - Skipped') AS result;
        END IF;
        
    ELSEIF operation_type_param = 'UPDATE' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.tconst'));
        SET v_title = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.primaryTitle'));
        SET v_runtime = JSON_EXTRACT(new_value_json, '$.runtimeMinutes');
        SET v_rating = JSON_EXTRACT(new_value_json, '$.averageRating');
        SET v_votes = JSON_EXTRACT(new_value_json, '$.numVotes');
        SET v_weighted = JSON_EXTRACT(new_value_json, '$.weightedRating');
        SET v_year = JSON_EXTRACT(new_value_json, '$.startYear');
        
        -- Update on Node A via federated table
        UPDATE title_ft_node_a 
        SET primaryTitle = v_title,
            runtimeMinutes = v_runtime,
            averageRating = v_rating,
            numVotes = v_votes,
            weightedRating = v_weighted,
            startYear = v_year
        WHERE tconst = v_tconst;
        
        SELECT CONCAT('✅ Replayed UPDATE to Node A: ', v_tconst) AS result;
        
    ELSEIF operation_type_param = 'DELETE' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(old_value_json, '$.tconst'));
        
        -- Delete from Node A via federated table
        DELETE FROM title_ft_node_a WHERE tconst = v_tconst;
        
        SELECT CONCAT('✅ Replayed DELETE to Node A: ', v_tconst) AS result;
        
    ELSE
        SELECT '❌ Unknown operation type' AS result;
    END IF;
END$$

DELIMITER ;

-- ============================================================================
-- 4. REPLAY TRANSACTION TO NODE B (Direct Insert)
-- ============================================================================
DROP PROCEDURE IF EXISTS replay_to_node_b;

DELIMITER $$

CREATE PROCEDURE replay_to_node_b(
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
        
        IF NOT EXISTS (SELECT 1 FROM title_ft_node_b WHERE tconst = v_tconst) THEN
            INSERT INTO title_ft_node_b (tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear)
            VALUES (v_tconst, v_title, v_runtime, v_rating, v_votes, v_weighted, v_year);
            
            SELECT CONCAT('✅ Replayed INSERT to Node B: ', v_tconst) AS result;
        ELSE
            SELECT CONCAT('⚠️ Record already exists on Node B: ', v_tconst, ' - Skipped') AS result;
        END IF;
        
    ELSEIF operation_type_param = 'UPDATE' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.tconst'));
        SET v_title = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.primaryTitle'));
        SET v_runtime = JSON_EXTRACT(new_value_json, '$.runtimeMinutes');
        SET v_rating = JSON_EXTRACT(new_value_json, '$.averageRating');
        SET v_votes = JSON_EXTRACT(new_value_json, '$.numVotes');
        SET v_weighted = JSON_EXTRACT(new_value_json, '$.weightedRating');
        SET v_year = JSON_EXTRACT(new_value_json, '$.startYear');
        
        UPDATE title_ft_node_b 
        SET primaryTitle = v_title,
            runtimeMinutes = v_runtime,
            averageRating = v_rating,
            numVotes = v_votes,
            weightedRating = v_weighted,
            startYear = v_year
        WHERE tconst = v_tconst;
        
        SELECT CONCAT('✅ Replayed UPDATE to Node B: ', v_tconst) AS result;
        
    ELSEIF operation_type_param = 'DELETE' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(old_value_json, '$.tconst'));
        
        DELETE FROM title_ft_node_b WHERE tconst = v_tconst;
        
        SELECT CONCAT('✅ Replayed DELETE to Node B: ', v_tconst) AS result;
        
    ELSE
        SELECT '❌ Unknown operation type' AS result;
    END IF;
END$$

DELIMITER ;

-- ============================================================================
-- 5. FULL RECOVERY FOR NODE A (Automated with Checkpoint)
-- ============================================================================
DROP PROCEDURE IF EXISTS full_recovery_node_a;

DELIMITER $$

CREATE PROCEDURE full_recovery_node_a(
    IN since_timestamp TIMESTAMP(6)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_operation VARCHAR(10);
    DECLARE v_new_value TEXT;
    DECLARE v_old_value TEXT;
    DECLARE v_tconst VARCHAR(12);
    DECLARE v_transaction_id VARCHAR(50);
    DECLARE v_max_timestamp TIMESTAMP(6);
    DECLARE recovery_count INT DEFAULT 0;
    DECLARE checkpoint_time TIMESTAMP(6);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Rollback on any error
        ROLLBACK;
        SELECT '❌ Recovery failed - transaction rolled back' AS result;
    END;
    
    -- Get last checkpoint
    SELECT GREATEST(last_recovery_timestamp, since_timestamp)
    INTO checkpoint_time
    FROM recovery_checkpoint
    WHERE node_name = 'node_a';
    
    -- Cursor for missing transactions
    DECLARE missing_cursor CURSOR FOR
        SELECT 
            COALESCE(tm.operation_type, prev_tm.operation_type) AS operation_type,
            COALESCE(tm.new_value, prev_tm.new_value) AS new_value,
            COALESCE(tm.old_value, prev_tm.old_value) AS old_value,
            COALESCE(tm.record_id, prev_tm.record_id) AS record_id,
            COALESCE(tm.transaction_id, prev_tm.transaction_id) AS transaction_id,
            COALESCE(tm.timestamp, prev_tm.timestamp) AS timestamp
        FROM transaction_log tm
        LEFT JOIN transaction_log prev_tm 
            ON tm.transaction_id = prev_tm.transaction_id 
            AND tm.log_sequence = prev_tm.log_sequence + 1
            AND prev_tm.new_value IS NOT NULL
        WHERE tm.log_type = 'MODIFY'
          AND tm.timestamp > checkpoint_time
          AND (
              (COALESCE(tm.operation_type, prev_tm.operation_type) = 'INSERT' AND COALESCE(tm.new_value, prev_tm.new_value) IS NOT NULL AND CAST(JSON_UNQUOTE(JSON_EXTRACT(COALESCE(tm.new_value, prev_tm.new_value), '$.startYear')) AS UNSIGNED) >= 2025)
              OR (COALESCE(tm.operation_type, prev_tm.operation_type) = 'UPDATE' AND COALESCE(tm.new_value, prev_tm.new_value) IS NOT NULL AND CAST(JSON_UNQUOTE(JSON_EXTRACT(COALESCE(tm.new_value, prev_tm.new_value), '$.startYear')) AS UNSIGNED) >= 2025)
              OR (COALESCE(tm.operation_type, prev_tm.operation_type) = 'DELETE' AND COALESCE(tm.old_value, prev_tm.old_value) IS NOT NULL AND CAST(JSON_UNQUOTE(JSON_EXTRACT(COALESCE(tm.old_value, prev_tm.old_value), '$.startYear')) AS UNSIGNED) >= 2025)
          )
        ORDER BY COALESCE(tm.timestamp, prev_tm.timestamp) ASC, tm.log_sequence ASC;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    SELECT CONCAT('🔄 Starting recovery for Node A from checkpoint: ', checkpoint_time) AS status;
    
    -- START TRANSACTION - All replays in one atomic operation
    START TRANSACTION;
    
    SET v_max_timestamp = checkpoint_time;
    
    OPEN missing_cursor;
    
    read_loop: LOOP
        FETCH missing_cursor INTO v_operation, v_new_value, v_old_value, v_tconst, v_transaction_id, v_max_timestamp;
        
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        -- Replay the transaction
        CALL replay_to_node_a(v_operation, v_new_value, v_old_value);
        SET recovery_count = recovery_count + 1;
        
    END LOOP;
    
    CLOSE missing_cursor;
    
    -- Update checkpoint to latest transaction timestamp
    UPDATE recovery_checkpoint
    SET last_recovery_timestamp = v_max_timestamp,
        recovery_count = recovery_count + recovery_count,
        last_transaction_id = v_transaction_id
    WHERE node_name = 'node_a';
    
    -- COMMIT - All replays succeeded
    COMMIT;
    
    SELECT CONCAT('✅ Recovery complete! Replayed ', recovery_count, ' transactions to Node A. Checkpoint saved at: ', v_max_timestamp) AS result;
END$$

DELIMITER ;

-- ============================================================================
-- 6. FULL RECOVERY FOR NODE B (Automated with Checkpoint)
-- ============================================================================
DROP PROCEDURE IF EXISTS full_recovery_node_b;

DELIMITER $$

CREATE PROCEDURE full_recovery_node_b(
    IN since_timestamp TIMESTAMP(6)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_operation VARCHAR(10);
    DECLARE v_new_value TEXT;
    DECLARE v_old_value TEXT;
    DECLARE v_tconst VARCHAR(12);
    DECLARE v_transaction_id VARCHAR(50);
    DECLARE v_max_timestamp TIMESTAMP(6);
    DECLARE recovery_count INT DEFAULT 0;
    DECLARE checkpoint_time TIMESTAMP(6);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Rollback on any error
        ROLLBACK;
        SELECT '❌ Recovery failed - transaction rolled back' AS result;
    END;
    
    -- Get last checkpoint
    SELECT GREATEST(last_recovery_timestamp, since_timestamp)
    INTO checkpoint_time
    FROM recovery_checkpoint
    WHERE node_name = 'node_b';
    
    DECLARE missing_cursor CURSOR FOR
        SELECT 
            tm.operation_type,
            tm.new_value,
            tm.old_value,
            tm.record_id,
            tm.transaction_id,
            tm.timestamp
        FROM transaction_log tm
        WHERE tm.log_type = 'COMMIT'
          AND tm.timestamp > checkpoint_time
          AND tm.operation_type IS NOT NULL
          AND (
              (tm.operation_type = 'INSERT' AND (JSON_EXTRACT(tm.new_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.new_value, '$.startYear') IS NULL))
              OR (tm.operation_type = 'UPDATE' AND (JSON_EXTRACT(tm.new_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.new_value, '$.startYear') IS NULL))
              OR (tm.operation_type = 'DELETE' AND (JSON_EXTRACT(tm.old_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.old_value, '$.startYear') IS NULL))
          )
        ORDER BY tm.timestamp ASC;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    SELECT CONCAT('🔄 Starting recovery for Node B from checkpoint: ', checkpoint_time) AS status;
    
    -- START TRANSACTION - All replays in one atomic operation
    START TRANSACTION;
    
    SET v_max_timestamp = checkpoint_time;
    
    OPEN missing_cursor;
    
    read_loop: LOOP
        FETCH missing_cursor INTO v_operation, v_new_value, v_old_value, v_tconst, v_transaction_id, v_max_timestamp;
        
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        CALL replay_to_node_b(v_operation, v_new_value, v_old_value);
        SET recovery_count = recovery_count + 1;
        
    END LOOP;
    
    CLOSE missing_cursor;
    
    -- Update checkpoint to latest transaction timestamp
    UPDATE recovery_checkpoint
    SET last_recovery_timestamp = v_max_timestamp,
        recovery_count = recovery_count + recovery_count,
        last_transaction_id = v_transaction_id
    WHERE node_name = 'node_b';
    
    -- COMMIT - All replays succeeded
    COMMIT;
    
    SELECT CONCAT('✅ Recovery complete! Replayed ', recovery_count, ' transactions to Node B. Checkpoint saved at: ', v_max_timestamp) AS result;
END$$

DELIMITER ;

SELECT '=== Node Recovery Procedures Created with Checkpoint System ===' AS status;
SELECT 'Recovery runs ONLY on startup, all replays in one transaction, checkpoint prevents duplicates' AS info;
