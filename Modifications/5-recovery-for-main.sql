-- ============================================================================
-- RECOVERY FOR MAIN (Run on MAIN)
-- Main reads Node A and B's transaction_log tables (via federated)
-- and replays missing transactions to its local title_ft table
-- Uses checkpoint system to track last recovery timestamp
-- ============================================================================

USE `stadvdb-mco2`;

-- ============================================================================
-- 0. CREATE RECOVERY CHECKPOINT TABLE (if not exists)
-- ============================================================================
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

SELECT '✅ Recovery checkpoint table ready for Main recovery' AS status;

-- ============================================================================
-- 1. FIND MISSING TRANSACTIONS ON MAIN (from this node's perspective)
-- (Removed: find_missing_on_main not needed for Main self-recovery)

-- ============================================================================
-- 1. REPLAY TRANSACTION TO MAIN'S LOCAL TABLE
-- ============================================================================
DROP PROCEDURE IF EXISTS replay_to_main;

DELIMITER $$

CREATE PROCEDURE replay_to_main(
    IN operation_type_param VARCHAR(10),
    IN new_value_json TEXT,
    IN old_value_json TEXT,
    IN record_id_param VARCHAR(12),
    IN transaction_id_param VARCHAR(50)
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
        
        -- Insert to Main's LOCAL table (not federated)
        INSERT INTO title_ft (tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear)
        VALUES (v_tconst, v_title, v_runtime, v_rating, v_votes, v_weighted, v_year)
        ON DUPLICATE KEY UPDATE
            primaryTitle = v_title,
            runtimeMinutes = v_runtime,
            averageRating = v_rating,
            numVotes = v_votes,
            weightedRating = v_weighted,
            startYear = v_year;
        
    ELSEIF operation_type_param = 'UPDATE' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.tconst'));
        SET v_title = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.primaryTitle'));
        SET v_runtime = JSON_EXTRACT(new_value_json, '$.runtimeMinutes');
        SET v_rating = JSON_EXTRACT(new_value_json, '$.averageRating');
        SET v_votes = JSON_EXTRACT(new_value_json, '$.numVotes');
        SET v_weighted = JSON_EXTRACT(new_value_json, '$.weightedRating');
        SET v_year = JSON_EXTRACT(new_value_json, '$.startYear');
        
        -- Update Main's LOCAL table (not federated)
        UPDATE title_ft 
        SET primaryTitle = v_title,
            runtimeMinutes = v_runtime,
            averageRating = v_rating,
            numVotes = v_votes,
            weightedRating = v_weighted,
            startYear = v_year
        WHERE tconst = v_tconst;
        
    ELSEIF operation_type_param = 'DELETE' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(old_value_json, '$.tconst'));
        
        -- Delete from Main's LOCAL table (not federated)
        DELETE FROM title_ft WHERE tconst = v_tconst;
    END IF;
END$$

DELIMITER ;

-- ============================================================================
-- 2. FULL RECOVERY FOR MAIN (reads from Node A and B's logs)
-- ============================================================================
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
    DECLARE v_record_id VARCHAR(12);
    DECLARE v_transaction_id VARCHAR(50);
    DECLARE v_timestamp TIMESTAMP(6);
    DECLARE v_max_timestamp TIMESTAMP(6);
    DECLARE recovery_count INT DEFAULT 0;
    DECLARE checkpoint_time TIMESTAMP(6);
    DECLARE federated_error INT DEFAULT 0;
    DECLARE total_replayed INT DEFAULT 0;
    
    -- Get last checkpoint
    SELECT GREATEST(IFNULL(last_recovery_timestamp, '2000-01-01'), since_timestamp)
    INTO checkpoint_time
    FROM recovery_checkpoint
    WHERE node_name = 'main';
    
    SELECT CONCAT('🔄 Starting Main recovery from checkpoint: ', checkpoint_time) AS status;
    
    SET v_max_timestamp = checkpoint_time;
    
    -- ========================================================================
    -- PHASE 1: Process Node A's transactions
    -- ========================================================================
    SELECT CONCAT('📊 Scanning Node A for MODIFY transactions...') AS debug_info;
    
    SELECT COUNT(*) INTO @transaction_count
    FROM transaction_log_node_a ta
    WHERE ta.log_type = 'MODIFY'
      AND ta.timestamp > checkpoint_time;
    
    SELECT CONCAT('📊 Found ', IFNULL(@transaction_count, 0), ' transactions from Node A') AS debug_info;
    
    BEGIN
        -- CURSOR MUST BE DECLARED BEFORE HANDLERS
        DECLARE cur CURSOR FOR
            SELECT 
                ta.operation_type,
                ta.new_value,
                ta.old_value,
                ta.record_id,
                ta.transaction_id,
                ta.timestamp
            FROM transaction_log_node_a ta
            WHERE ta.log_type = 'MODIFY'
              AND ta.timestamp > checkpoint_time
            ORDER BY ta.timestamp ASC, ta.log_sequence ASC;
        
        -- HANDLERS DECLARED AFTER CURSOR
        DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
        BEGIN
            SET federated_error = 1;
        END;
        
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        
        OPEN cur;
        
        read_loop_a: LOOP
            FETCH cur INTO v_operation, v_new_value, v_old_value, v_record_id, v_transaction_id, v_timestamp;
            
            IF done THEN
                LEAVE read_loop_a;
            END IF;
            
            BEGIN
                DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
                BEGIN
                    SELECT CONCAT('⚠️ Error processing ', v_transaction_id, ' - ', v_operation, ' on ', v_record_id) AS error_msg;
                END;
                
                CALL replay_to_main(v_operation, v_new_value, v_old_value, v_record_id, v_transaction_id);
                SET recovery_count = recovery_count + 1;
                SET v_max_timestamp = v_timestamp;
            END;
            
        END LOOP;
        
        CLOSE cur;
    END;
    
    SET total_replayed = recovery_count;
    SELECT CONCAT('✅ Node A recovery complete! Replayed ', recovery_count, ' transactions') AS node_a_result;
    
    -- ========================================================================
    -- PHASE 2: Process Node B's transactions
    -- ========================================================================
    SELECT CONCAT('📊 Scanning Node B for MODIFY transactions...') AS debug_info;
    
    SELECT COUNT(*) INTO @transaction_count
    FROM transaction_log_node_b tb
    WHERE tb.log_type = 'MODIFY'
      AND tb.timestamp > checkpoint_time;
    
    SELECT CONCAT('📊 Found ', IFNULL(@transaction_count, 0), ' transactions from Node B') AS debug_info;
    
    SET done = FALSE;
    SET recovery_count = 0;
    
    BEGIN
        -- CURSOR MUST BE DECLARED BEFORE HANDLERS
        DECLARE cur_b CURSOR FOR
            SELECT 
                tb.operation_type,
                tb.new_value,
                tb.old_value,
                tb.record_id,
                tb.transaction_id,
                tb.timestamp
            FROM transaction_log_node_b tb
            WHERE tb.log_type = 'MODIFY'
              AND tb.timestamp > checkpoint_time
            ORDER BY tb.timestamp ASC, tb.log_sequence ASC;
        
        -- HANDLERS DECLARED AFTER CURSOR
        DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
        BEGIN
            SET federated_error = 1;
        END;
        
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        
        OPEN cur_b;
        
        read_loop_b: LOOP
            FETCH cur_b INTO v_operation, v_new_value, v_old_value, v_record_id, v_transaction_id, v_timestamp;
            
            IF done THEN
                LEAVE read_loop_b;
            END IF;
            
            BEGIN
                DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
                BEGIN
                    SELECT CONCAT('⚠️ Error processing ', v_transaction_id, ' - ', v_operation, ' on ', v_record_id) AS error_msg;
                END;
                
                CALL replay_to_main(v_operation, v_new_value, v_old_value, v_record_id, v_transaction_id);
                SET recovery_count = recovery_count + 1;
                SET v_max_timestamp = v_timestamp;
            END;
            
        END LOOP;
        
        CLOSE cur_b;
    END;
    
    SET total_replayed = total_replayed + recovery_count;
    SELECT CONCAT('✅ Node B recovery complete! Replayed ', recovery_count, ' transactions') AS node_b_result;
    
    -- ========================================================================
    -- Update checkpoint with final timestamp
    -- ========================================================================
    IF total_replayed > 0 THEN
        UPDATE recovery_checkpoint
        SET last_recovery_timestamp = v_max_timestamp,
            recovery_count = recovery_count + IFNULL(recovery_count, 0),
            last_transaction_id = v_transaction_id
        WHERE node_name = 'main';
    END IF;
    
    SELECT CONCAT('✅ Main recovery complete! Total replayed: ', total_replayed, ' transactions. Checkpoint saved at: ', v_max_timestamp) AS final_result;
END$$

DELIMITER ;

SELECT '=== Main Self-Recovery Procedure Created ===' AS status;
SELECT 'Run on Main:' AS info;
SELECT '  SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = "main";' AS step1;
SELECT '  CALL full_recovery_main(@checkpoint);' AS step2;
