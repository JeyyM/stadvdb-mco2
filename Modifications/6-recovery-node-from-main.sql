-- ============================================================================
-- RECOVERY: Use Main's Logs to Recover Node A or Node B
-- Case #4: Node A/B recovers from failure and missed transactions
-- ============================================================================

USE `stadvdb-mco2`;

-- ============================================================================
-- 1. FIND MISSING TRANSACTIONS ON NODE A (compared to Main's log)
-- ============================================================================
DROP PROCEDURE IF EXISTS find_missing_on_node_a;

DELIMITER $$

CREATE PROCEDURE find_missing_on_node_a(
    IN since_timestamp TIMESTAMP(6)
)
BEGIN
    -- Find committed transactions in Main's log that should have gone to Node A
    -- but are missing from Node A's log
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
      AND tm.timestamp >= since_timestamp
      AND tm.operation_type IS NOT NULL
      -- Check if this record should be on Node A (startYear >= 2025)
      AND (
          (tm.operation_type = 'INSERT' AND JSON_EXTRACT(tm.new_value, '$.startYear') >= 2025)
          OR (tm.operation_type = 'UPDATE' AND JSON_EXTRACT(tm.new_value, '$.startYear') >= 2025)
          OR (tm.operation_type = 'DELETE' AND JSON_EXTRACT(tm.old_value, '$.startYear') >= 2025)
      )
      -- Check if it's missing from Node A's log
      AND NOT EXISTS (
        SELECT 1 FROM transaction_log_node_a tl_a
        WHERE tl_a.record_id = tm.record_id
          AND tl_a.timestamp BETWEEN tm.timestamp - INTERVAL 5 SECOND
                                 AND tm.timestamp + INTERVAL 5 SECOND
          AND tl_a.log_type = 'COMMIT'
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
    -- Find committed transactions in Main's log that should have gone to Node B
    -- but are missing from Node B's log
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
      AND tm.timestamp >= since_timestamp
      AND tm.operation_type IS NOT NULL
      -- Check if this record should be on Node B (startYear < 2025 or NULL)
      AND (
          (tm.operation_type = 'INSERT' AND (JSON_EXTRACT(tm.new_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.new_value, '$.startYear') IS NULL))
          OR (tm.operation_type = 'UPDATE' AND (JSON_EXTRACT(tm.new_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.new_value, '$.startYear') IS NULL))
          OR (tm.operation_type = 'DELETE' AND (JSON_EXTRACT(tm.old_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.old_value, '$.startYear') IS NULL))
      )
      -- Check if it's missing from Node B's log
      AND NOT EXISTS (
        SELECT 1 FROM transaction_log_node_b tl_b
        WHERE tl_b.record_id = tm.record_id
          AND tl_b.timestamp BETWEEN tm.timestamp - INTERVAL 5 SECOND
                                 AND tm.timestamp + INTERVAL 5 SECOND
          AND tl_b.log_type = 'COMMIT'
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
-- 5. FULL RECOVERY FOR NODE A (Automated)
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
    DECLARE recovery_count INT DEFAULT 0;
    
    -- Cursor for missing transactions
    DECLARE missing_cursor CURSOR FOR
        SELECT 
            tm.operation_type,
            tm.new_value,
            tm.old_value,
            tm.record_id
        FROM transaction_log tm
        WHERE tm.log_type = 'COMMIT'
          AND tm.timestamp >= since_timestamp
          AND tm.operation_type IS NOT NULL
          AND (
              (tm.operation_type = 'INSERT' AND JSON_EXTRACT(tm.new_value, '$.startYear') >= 2025)
              OR (tm.operation_type = 'UPDATE' AND JSON_EXTRACT(tm.new_value, '$.startYear') >= 2025)
              OR (tm.operation_type = 'DELETE' AND JSON_EXTRACT(tm.old_value, '$.startYear') >= 2025)
          )
          AND NOT EXISTS (
            SELECT 1 FROM title_ft_node_a tl_a
            WHERE tl_a.tconst = tm.record_id
          )
        ORDER BY tm.timestamp ASC;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    SELECT CONCAT('🔄 Starting recovery for Node A from timestamp: ', since_timestamp) AS status;
    
    OPEN missing_cursor;
    
    read_loop: LOOP
        FETCH missing_cursor INTO v_operation, v_new_value, v_old_value, v_tconst;
        
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        -- Replay the transaction
        CALL replay_to_node_a(v_operation, v_new_value, v_old_value);
        SET recovery_count = recovery_count + 1;
        
    END LOOP;
    
    CLOSE missing_cursor;
    
    SELECT CONCAT('✅ Recovery complete! Replayed ', recovery_count, ' transactions to Node A') AS result;
END$$

DELIMITER ;

-- ============================================================================
-- 6. FULL RECOVERY FOR NODE B (Automated)
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
    DECLARE recovery_count INT DEFAULT 0;
    
    DECLARE missing_cursor CURSOR FOR
        SELECT 
            tm.operation_type,
            tm.new_value,
            tm.old_value,
            tm.record_id
        FROM transaction_log tm
        WHERE tm.log_type = 'COMMIT'
          AND tm.timestamp >= since_timestamp
          AND tm.operation_type IS NOT NULL
          AND (
              (tm.operation_type = 'INSERT' AND (JSON_EXTRACT(tm.new_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.new_value, '$.startYear') IS NULL))
              OR (tm.operation_type = 'UPDATE' AND (JSON_EXTRACT(tm.new_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.new_value, '$.startYear') IS NULL))
              OR (tm.operation_type = 'DELETE' AND (JSON_EXTRACT(tm.old_value, '$.startYear') < 2025 OR JSON_EXTRACT(tm.old_value, '$.startYear') IS NULL))
          )
          AND NOT EXISTS (
            SELECT 1 FROM title_ft_node_b tl_b
            WHERE tl_b.tconst = tm.record_id
          )
        ORDER BY tm.timestamp ASC;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    SELECT CONCAT('🔄 Starting recovery for Node B from timestamp: ', since_timestamp) AS status;
    
    OPEN missing_cursor;
    
    read_loop: LOOP
        FETCH missing_cursor INTO v_operation, v_new_value, v_old_value, v_tconst;
        
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        CALL replay_to_node_b(v_operation, v_new_value, v_old_value);
        SET recovery_count = recovery_count + 1;
        
    END LOOP;
    
    CLOSE missing_cursor;
    
    SELECT CONCAT('✅ Recovery complete! Replayed ', recovery_count, ' transactions to Node B') AS result;
END$$

DELIMITER ;

SELECT '=== Node Recovery Procedures Created ===' AS status;
SELECT 'Use full_recovery_node_a() or full_recovery_node_b() to recover nodes from Main logs' AS info;
