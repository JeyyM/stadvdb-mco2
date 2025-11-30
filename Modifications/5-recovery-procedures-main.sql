-- ============================================================================
-- RECOVERY PROCEDURES FOR MAIN NODE
-- These procedures help recover from node failures and synchronize data
-- ============================================================================

USE `stadvdb-mco2`;

-- ============================================================================
-- 1. CHECK FOR MISSING TRANSACTIONS FROM NODE A
-- ============================================================================
DROP PROCEDURE IF EXISTS check_missing_from_node_a;

DELIMITER $$

CREATE PROCEDURE check_missing_from_node_a(
    IN since_timestamp TIMESTAMP(6)
)
BEGIN
    -- Find transactions committed on Node A but not present in Main
    SELECT 
        tl_a.transaction_id,
        tl_a.timestamp,
        tl_a.operation_type,
        tl_a.record_id,
        tl_a.new_value,
        tl_a.old_value
    FROM transaction_log_node_a tl_a
    WHERE tl_a.log_type = 'COMMIT'
      AND tl_a.timestamp >= since_timestamp
      AND tl_a.operation_type IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM transaction_log tm
        WHERE tm.record_id = tl_a.record_id
          AND tm.timestamp BETWEEN tl_a.timestamp - INTERVAL 2 SECOND
                               AND tl_a.timestamp + INTERVAL 2 SECOND
          AND tm.source_node = 'NODE_A'
      )
    ORDER BY tl_a.timestamp;
END$$

DELIMITER ;

-- ============================================================================
-- 2. CHECK FOR MISSING TRANSACTIONS FROM NODE B
-- ============================================================================
DROP PROCEDURE IF EXISTS check_missing_from_node_b;

DELIMITER $$

CREATE PROCEDURE check_missing_from_node_b(
    IN since_timestamp TIMESTAMP(6)
)
BEGIN
    -- Find transactions committed on Node B but not present in Main
    SELECT 
        tl_b.transaction_id,
        tl_b.timestamp,
        tl_b.operation_type,
        tl_b.record_id,
        tl_b.new_value,
        tl_b.old_value
    FROM transaction_log_node_b tl_b
    WHERE tl_b.log_type = 'COMMIT'
      AND tl_b.timestamp >= since_timestamp
      AND tl_b.operation_type IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM transaction_log tm
        WHERE tm.record_id = tl_b.record_id
          AND tm.timestamp BETWEEN tl_b.timestamp - INTERVAL 2 SECOND
                               AND tl_b.timestamp + INTERVAL 2 SECOND
          AND tm.source_node = 'NODE_B'
      )
    ORDER BY tl_b.timestamp;
END$$

DELIMITER ;

-- ============================================================================
-- 3. REPLAY TRANSACTION FROM LOG (Manual recovery helper)
-- ============================================================================
DROP PROCEDURE IF EXISTS replay_transaction;

DELIMITER $$

CREATE PROCEDURE replay_transaction(
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
    DECLARE v_year SMALLINT UNSIGNED;
    
    IF operation_type_param = 'INSERT' THEN
        -- Extract values from JSON
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.tconst'));
        SET v_title = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.primaryTitle'));
        SET v_runtime = JSON_EXTRACT(new_value_json, '$.runtimeMinutes');
        SET v_rating = JSON_EXTRACT(new_value_json, '$.averageRating');
        SET v_votes = JSON_EXTRACT(new_value_json, '$.numVotes');
        SET v_year = JSON_EXTRACT(new_value_json, '$.startYear');
        
        -- Check if record already exists
        IF NOT EXISTS (SELECT 1 FROM title_ft WHERE tconst = v_tconst) THEN
            CALL distributed_insert(v_tconst, v_title, v_runtime, v_rating, v_votes, v_year);
            SELECT CONCAT('Replayed INSERT for ', v_tconst) AS result;
        ELSE
            SELECT CONCAT('Record ', v_tconst, ' already exists, skipped') AS result;
        END IF;
        
    ELSEIF operation_type_param = 'UPDATE' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.tconst'));
        SET v_title = JSON_UNQUOTE(JSON_EXTRACT(new_value_json, '$.primaryTitle'));
        SET v_runtime = JSON_EXTRACT(new_value_json, '$.runtimeMinutes');
        SET v_rating = JSON_EXTRACT(new_value_json, '$.averageRating');
        SET v_votes = JSON_EXTRACT(new_value_json, '$.numVotes');
        SET v_year = JSON_EXTRACT(new_value_json, '$.startYear');
        
        CALL distributed_update(v_tconst, v_title, v_runtime, v_rating, v_votes, v_year);
        SELECT CONCAT('Replayed UPDATE for ', v_tconst) AS result;
        
    ELSEIF operation_type_param = 'DELETE' THEN
        SET v_tconst = JSON_UNQUOTE(JSON_EXTRACT(old_value_json, '$.tconst'));
        
        IF EXISTS (SELECT 1 FROM title_ft WHERE tconst = v_tconst) THEN
            CALL distributed_delete(v_tconst);
            SELECT CONCAT('Replayed DELETE for ', v_tconst) AS result;
        ELSE
            SELECT CONCAT('Record ', v_tconst, ' already deleted, skipped') AS result;
        END IF;
    ELSE
        SELECT 'Unknown operation type' AS result;
    END IF;
END$$

DELIMITER ;

-- ============================================================================
-- 4. HEALTH CHECK - Compare record counts
-- ============================================================================
DROP PROCEDURE IF EXISTS health_check_counts;

DELIMITER $$

CREATE PROCEDURE health_check_counts()
BEGIN
    SELECT 
        'Main' AS node,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN startYear >= 2025 THEN 1 END) AS node_a_partition,
        COUNT(CASE WHEN startYear < 2025 OR startYear IS NULL THEN 1 END) AS node_b_partition
    FROM title_ft
    
    UNION ALL
    
    SELECT 
        'Node A (via federated)',
        COUNT(*),
        COUNT(*),
        0
    FROM title_ft_node_a
    
    UNION ALL
    
    SELECT 
        'Node B (via federated)',
        COUNT(*),
        0,
        COUNT(*)
    FROM title_ft_node_b;
END$$

DELIMITER ;

-- ============================================================================
-- 5. CHECK FOR UNCOMMITTED TRANSACTIONS
-- ============================================================================
DROP PROCEDURE IF EXISTS check_uncommitted_transactions;

DELIMITER $$

CREATE PROCEDURE check_uncommitted_transactions()
BEGIN
    -- Check Main's log
    SELECT 'MAIN' AS node, t1.transaction_id, t1.timestamp, t1.source_node
    FROM transaction_log t1
    WHERE t1.log_type = 'BEGIN'
      AND NOT EXISTS (
        SELECT 1 FROM transaction_log t2
        WHERE t2.transaction_id = t1.transaction_id
          AND t2.log_type IN ('COMMIT', 'ABORT')
      )
    
    UNION ALL
    
    -- Check Node A's log
    SELECT 'NODE_A', t1.transaction_id, t1.timestamp, t1.source_node
    FROM transaction_log_node_a t1
    WHERE t1.log_type = 'BEGIN'
      AND NOT EXISTS (
        SELECT 1 FROM transaction_log_node_a t2
        WHERE t2.transaction_id = t1.transaction_id
          AND t2.log_type IN ('COMMIT', 'ABORT')
      )
    
    UNION ALL
    
    -- Check Node B's log
    SELECT 'NODE_B', t1.transaction_id, t1.timestamp, t1.source_node
    FROM transaction_log_node_b t1
    WHERE t1.log_type = 'BEGIN'
      AND NOT EXISTS (
        SELECT 1 FROM transaction_log_node_b t2
        WHERE t2.transaction_id = t1.transaction_id
          AND t2.log_type IN ('COMMIT', 'ABORT')
      )
    
    ORDER BY timestamp DESC;
END$$

DELIMITER ;

-- ============================================================================
-- 6. CHECK FOR ABORTED TRANSACTIONS
-- ============================================================================
DROP PROCEDURE IF EXISTS check_aborted_transactions;

DELIMITER $$

CREATE PROCEDURE check_aborted_transactions(
    IN hours_back INT
)
BEGIN
    SELECT 
        'MAIN' AS node,
        transaction_id, 
        timestamp, 
        source_node, 
        operation_type,
        record_id
    FROM transaction_log
    WHERE log_type = 'ABORT'
      AND timestamp > NOW() - INTERVAL hours_back HOUR
    
    UNION ALL
    
    SELECT 
        'NODE_A',
        transaction_id, 
        timestamp, 
        source_node, 
        operation_type,
        record_id
    FROM transaction_log_node_a
    WHERE log_type = 'ABORT'
      AND timestamp > NOW() - INTERVAL hours_back HOUR
    
    UNION ALL
    
    SELECT 
        'NODE_B',
        transaction_id, 
        timestamp, 
        source_node, 
        operation_type,
        record_id
    FROM transaction_log_node_b
    WHERE log_type = 'ABORT'
      AND timestamp > NOW() - INTERVAL hours_back HOUR
    
    ORDER BY timestamp DESC;
END$$

DELIMITER ;

-- ============================================================================
-- 7. GET TRANSACTION DETAILS
-- ============================================================================
DROP PROCEDURE IF EXISTS get_transaction_details;

DELIMITER $$

CREATE PROCEDURE get_transaction_details(
    IN txn_id VARCHAR(36)
)
BEGIN
    -- Get from Main
    SELECT 'MAIN' AS source, tl.*
    FROM transaction_log tl
    WHERE transaction_id = txn_id
    
    UNION ALL
    
    -- Get from Node A
    SELECT 'NODE_A', tl.*
    FROM transaction_log_node_a tl
    WHERE transaction_id = txn_id
    
    UNION ALL
    
    -- Get from Node B
    SELECT 'NODE_B', tl.*
    FROM transaction_log_node_b tl
    WHERE transaction_id = txn_id
    
    ORDER BY log_sequence;
END$$

DELIMITER ;

SELECT '=== Recovery procedures created on MAIN node ===' AS status;
SELECT 'Run these procedures to monitor and recover from failures' AS info;
