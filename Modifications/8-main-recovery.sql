-- ============================================================================
-- MAIN NODE RECOVERY PROCEDURES
-- Recovers Main node from Node A's transaction logs after failover
-- ============================================================================

USE `stadvdb-mco2`;

DROP PROCEDURE IF EXISTS recover_from_node_a;
DROP PROCEDURE IF EXISTS replay_transaction_from_log;

DELIMITER $$

-- ============================================================================
-- RECOVER FROM NODE A - Main recovery procedure after coming back online
-- With checkpoint-based batch recovery (idempotent, no UUID spam)
-- ============================================================================

CREATE PROCEDURE recover_from_node_a()
proc_label: BEGIN
    DECLARE checkpoint_timestamp TIMESTAMP(6);
    DECLARE last_node_a_timestamp TIMESTAMP(6);
    DECLARE recovery_count INT DEFAULT 0;
    DECLARE batch_transaction_id VARCHAR(36);
    DECLARE done INT DEFAULT FALSE;
    
    DECLARE txn_id VARCHAR(36);
    DECLARE txn_operation VARCHAR(10);
    DECLARE txn_record_id VARCHAR(255);
    DECLARE txn_new_value TEXT;
    DECLARE txn_timestamp TIMESTAMP(6);
    
    -- Cursor to fetch missed transactions from Node A since checkpoint
    DECLARE node_a_cursor CURSOR FOR
        SELECT DISTINCT transaction_id, operation_type, record_id, new_value, timestamp
        FROM transaction_log_node_a
        WHERE timestamp > checkpoint_timestamp
        AND source_node IN ('NODE_A_ACTING', 'NODE_A')
        AND log_type = 'MODIFY'
        AND table_name = 'title_ft'
        ORDER BY timestamp, log_sequence;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    -- ========================================================================
    -- STEP 1: Check checkpoint - has recovery already been done?
    -- ========================================================================
    
    SELECT IFNULL(last_recovered_timestamp, '2000-01-01 00:00:00.000000') 
    INTO checkpoint_timestamp
    FROM recovery_checkpoint
    WHERE node_name = 'NODE_A'
    LIMIT 1;
    
    -- Get Node A's latest transaction timestamp
    SELECT IFNULL(MAX(timestamp), '2000-01-01 00:00:00.000000') 
    INTO last_node_a_timestamp
    FROM transaction_log_node_a
    WHERE source_node IN ('NODE_A_ACTING', 'NODE_A')
    AND log_type = 'MODIFY';
    
    -- If checkpoint is already at or past Node A's latest transaction, we're done
    IF checkpoint_timestamp >= last_node_a_timestamp THEN
        SELECT CONCAT('✓ Already recovered. Checkpoint: ', checkpoint_timestamp, 
                     ', Node A latest: ', last_node_a_timestamp) AS status;
        SELECT 'No recovery needed - already up to date.' AS result;
        LEAVE proc_label;
    END IF;
    
    SELECT CONCAT('Recovery needed. Checkpoint: ', checkpoint_timestamp, 
                 ', Node A latest: ', last_node_a_timestamp) AS recovery_start;
    SELECT 'Fetching missed transactions from Node A...' AS status;
    
    -- ========================================================================
    -- STEP 2: Batch all operations into ONE transaction
    -- ========================================================================
    
    -- Generate ONE UUID for the entire batch
    SET batch_transaction_id = UUID();
    
    -- Start the batch transaction
    START TRANSACTION;
    
    -- Open cursor and replay all operations in this single transaction
    OPEN node_a_cursor;
    
    recovery_loop: LOOP
        FETCH node_a_cursor INTO txn_id, txn_operation, txn_record_id, txn_new_value, txn_timestamp;
        
        IF done THEN
            LEAVE recovery_loop;
        END IF;
        
        -- Replay the transaction within the batch
        BEGIN
            DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
            BEGIN
                -- Log failed replay but continue with batch
                SELECT CONCAT('Warning: Could not replay transaction ', txn_id, 
                            ' for record ', txn_record_id) AS warning;
            END;
            
            IF txn_operation = 'INSERT' THEN
                -- Extract values from JSON and insert
                INSERT INTO `stadvdb-mco2`.title_ft (
                    tconst, primaryTitle, runtimeMinutes, averageRating, 
                    numVotes, weightedRating, startYear
                )
                SELECT 
                    JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.tconst')),
                    JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.primaryTitle')),
                    JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.runtimeMinutes')),
                    JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.averageRating')),
                    JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.numVotes')),
                    JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.weightedRating')),
                    JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.startYear'))
                FROM DUAL
                WHERE NOT EXISTS (
                    SELECT 1 FROM `stadvdb-mco2`.title_ft WHERE tconst = txn_record_id
                );
                
            ELSEIF txn_operation = 'UPDATE' THEN
                -- Extract values from JSON and update
                UPDATE `stadvdb-mco2`.title_ft
                SET 
                    primaryTitle = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.primaryTitle')),
                    runtimeMinutes = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.runtimeMinutes')),
                    averageRating = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.averageRating')),
                    numVotes = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.numVotes')),
                    weightedRating = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.weightedRating')),
                    startYear = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.startYear'))
                WHERE tconst = txn_record_id;
                
            ELSEIF txn_operation = 'DELETE' THEN
                -- Delete the record
                DELETE FROM `stadvdb-mco2`.title_ft
                WHERE tconst = txn_record_id;
            END IF;
            
            SET recovery_count = recovery_count + 1;
        END;
        
    END LOOP;
    
    CLOSE node_a_cursor;
    
    -- ========================================================================
    -- STEP 3: Log single BATCH_RECOVERY entry (one UUID for everything)
    -- ========================================================================
    
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (batch_transaction_id, 1, 'BEGIN', 'MAIN_RECOVERY', NOW(6));
    
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, table_name, operation_type, 
     record_id, new_value, source_node, timestamp)
    VALUES (batch_transaction_id, 2, 'MODIFY', 'recovery_stats', 'BATCH_RECOVERY',
            'NODE_A', CONCAT('Recovered ', recovery_count, ' operations'), 
            'MAIN_RECOVERY', NOW(6));
    
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (batch_transaction_id, 3, 'COMMIT', 'MAIN_RECOVERY', NOW(6));
    
    -- ========================================================================
    -- STEP 4: Create/Update checkpoint - prevents re-processing
    -- ========================================================================
    
    INSERT INTO recovery_checkpoint (node_name, last_recovered_timestamp, 
                                     last_transaction_id, records_recovered)
    VALUES ('NODE_A', last_node_a_timestamp, batch_transaction_id, recovery_count)
    ON DUPLICATE KEY UPDATE
        last_recovered_timestamp = last_node_a_timestamp,
        last_transaction_id = batch_transaction_id,
        records_recovered = recovery_count,
        updated_at = CURRENT_TIMESTAMP;
    
    -- ========================================================================
    -- STEP 5: COMMIT the entire batch atomically
    -- ========================================================================
    
    COMMIT;
    
    SELECT CONCAT('✓ Batch recovery complete. Replayed ', recovery_count, 
                 ' transactions from Node A in single atomic batch.') AS status;
    SELECT CONCAT('Checkpoint updated to: ', last_node_a_timestamp) AS checkpoint;
    SELECT CONCAT('Transaction ID: ', batch_transaction_id) AS batch_id;
    SELECT 'Next step: Call demote_to_vice() on Node A to restore normal operation.' AS next_action;
END$$

-- ============================================================================
-- HELPER: Replay a single transaction from log entry
-- ============================================================================

CREATE PROCEDURE replay_transaction_from_log(
    IN txn_operation VARCHAR(10),
    IN txn_record_id VARCHAR(255),
    IN txn_new_value TEXT
)
BEGIN
    IF txn_operation = 'INSERT' THEN
        INSERT INTO `stadvdb-mco2`.title_ft (
            tconst, primaryTitle, runtimeMinutes, averageRating, 
            numVotes, weightedRating, startYear
        )
        SELECT 
            JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.tconst')),
            JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.primaryTitle')),
            JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.runtimeMinutes')),
            JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.averageRating')),
            JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.numVotes')),
            JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.weightedRating')),
            JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.startYear'))
        FROM DUAL;
        
    ELSEIF txn_operation = 'UPDATE' THEN
        UPDATE `stadvdb-mco2`.title_ft
        SET 
            primaryTitle = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.primaryTitle')),
            runtimeMinutes = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.runtimeMinutes')),
            averageRating = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.averageRating')),
            numVotes = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.numVotes')),
            weightedRating = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.weightedRating')),
            startYear = JSON_UNQUOTE(JSON_EXTRACT(txn_new_value, '$.startYear'))
        WHERE tconst = txn_record_id;
        
    ELSEIF txn_operation = 'DELETE' THEN
        DELETE FROM `stadvdb-mco2`.title_ft
        WHERE tconst = txn_record_id;
    END IF;
END$$

DELIMITER ;

SELECT 'Main node recovery procedures created successfully' AS status;
