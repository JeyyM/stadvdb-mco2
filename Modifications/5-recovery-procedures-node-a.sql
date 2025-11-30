-- ============================================================================
-- RECOVERY PROCEDURES FOR NODE A
-- These procedures help Node A recover and synchronize with Main
-- ============================================================================

USE `stadvdb-mco2-a`;

-- ============================================================================
-- 1. SYNC FROM MAIN - Full synchronization
-- ============================================================================
DROP PROCEDURE IF EXISTS sync_from_main;

DELIMITER $$

CREATE PROCEDURE sync_from_main()
BEGIN
    DECLARE synced_count INT DEFAULT 0;
    DECLARE updated_count INT DEFAULT 0;
    DECLARE deleted_count INT DEFAULT 0;
    
    START TRANSACTION;
    
    -- Insert missing records from Main that belong to Node A (startYear >= 2025)
    INSERT IGNORE INTO title_ft
    SELECT m.*
    FROM title_ft_main m
    WHERE m.startYear >= 2025
      AND NOT EXISTS (
        SELECT 1 FROM title_ft a
        WHERE a.tconst = m.tconst
      );
    
    SET synced_count = ROW_COUNT();
    
    -- Update records that differ from Main
    UPDATE title_ft a
    INNER JOIN title_ft_main m ON a.tconst = m.tconst
    SET a.primaryTitle = m.primaryTitle,
        a.runtimeMinutes = m.runtimeMinutes,
        a.averageRating = m.averageRating,
        a.numVotes = m.numVotes,
        a.weightedRating = m.weightedRating,
        a.startYear = m.startYear
    WHERE m.startYear >= 2025
      AND (a.primaryTitle != m.primaryTitle
        OR a.runtimeMinutes != m.runtimeMinutes
        OR a.numVotes != m.numVotes
        OR a.averageRating != m.averageRating
        OR a.weightedRating != m.weightedRating
        OR a.startYear != m.startYear);
    
    SET updated_count = ROW_COUNT();
    
    -- Remove records that don't belong to Node A anymore
    DELETE a FROM title_ft a
    LEFT JOIN title_ft_main m ON a.tconst = m.tconst
    WHERE a.startYear < 2025 
       OR a.startYear IS NULL
       OR m.tconst IS NULL;  -- Record deleted from Main
    
    SET deleted_count = ROW_COUNT();
    
    COMMIT;
    
    SELECT 
        'Node A sync completed' AS status,
        synced_count AS records_inserted,
        updated_count AS records_updated,
        deleted_count AS records_removed;
END$$

DELIMITER ;

-- ============================================================================
-- 2. CHECK MISSING TRANSACTIONS - What Main might have missed
-- ============================================================================
DROP PROCEDURE IF EXISTS check_missing_in_main;

DELIMITER $$

CREATE PROCEDURE check_missing_in_main(
    IN since_timestamp TIMESTAMP(6)
)
BEGIN
    -- Find committed transactions in Node A that aren't in Main's log
    SELECT 
        tl.transaction_id,
        tl.timestamp,
        tl.operation_type,
        tl.record_id,
        tl.new_value,
        tl.old_value
    FROM transaction_log tl
    WHERE tl.log_type = 'COMMIT'
      AND tl.timestamp >= since_timestamp
      AND tl.operation_type IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM transaction_log_main tm
        WHERE tm.record_id = tl.record_id
          AND tm.timestamp BETWEEN tl.timestamp - INTERVAL 2 SECOND
                               AND tl.timestamp + INTERVAL 2 SECOND
      )
    ORDER BY tl.timestamp;
END$$

DELIMITER ;

-- ============================================================================
-- 3. VERIFY DATA CONSISTENCY
-- ============================================================================
DROP PROCEDURE IF EXISTS verify_consistency;

DELIMITER $$

CREATE PROCEDURE verify_consistency()
BEGIN
    -- Check for records in Node A that don't match Main
    SELECT 
        'MISMATCH' AS status,
        a.tconst,
        a.primaryTitle AS node_a_title,
        m.primaryTitle AS main_title,
        a.numVotes AS node_a_votes,
        m.numVotes AS main_votes,
        a.averageRating AS node_a_rating,
        m.averageRating AS main_rating
    FROM title_ft a
    INNER JOIN title_ft_main m ON a.tconst = m.tconst
    WHERE a.startYear >= 2025
      AND (a.primaryTitle != m.primaryTitle
        OR a.numVotes != m.numVotes
        OR a.averageRating != m.averageRating
        OR a.weightedRating != m.weightedRating)
    LIMIT 100;
    
    -- Summary counts
    SELECT 
        (SELECT COUNT(*) FROM title_ft) AS node_a_count,
        (SELECT COUNT(*) FROM title_ft_main WHERE startYear >= 2025) AS main_node_a_partition,
        (SELECT COUNT(*) FROM title_ft WHERE startYear < 2025 OR startYear IS NULL) AS wrong_partition
    AS summary;
END$$

DELIMITER ;

-- ============================================================================
-- 4. CHECK LOCAL HEALTH
-- ============================================================================
DROP PROCEDURE IF EXISTS health_check;

DELIMITER $$

CREATE PROCEDURE health_check()
BEGIN
    -- Get counts
    SELECT 
        COUNT(*) AS total_records,
        COUNT(CASE WHEN startYear >= 2025 THEN 1 END) AS correct_partition,
        COUNT(CASE WHEN startYear < 2025 OR startYear IS NULL THEN 1 END) AS wrong_partition,
        MIN(startYear) AS min_year,
        MAX(startYear) AS max_year
    FROM title_ft;
    
    -- Check for uncommitted transactions
    SELECT COUNT(*) AS uncommitted_count
    FROM transaction_log t1
    WHERE t1.log_type = 'BEGIN'
      AND NOT EXISTS (
        SELECT 1 FROM transaction_log t2
        WHERE t2.transaction_id = t1.transaction_id
          AND t2.log_type IN ('COMMIT', 'ABORT')
      );
    
    -- Check for recent aborts
    SELECT COUNT(*) AS recent_aborts
    FROM transaction_log
    WHERE log_type = 'ABORT'
      AND timestamp > NOW() - INTERVAL 1 HOUR;
END$$

DELIMITER ;

-- ============================================================================
-- 5. GET RECENT TRANSACTIONS
-- ============================================================================
DROP PROCEDURE IF EXISTS get_recent_transactions;

DELIMITER $$

CREATE PROCEDURE get_recent_transactions(
    IN hours_back INT,
    IN limit_count INT
)
BEGIN
    SELECT 
        transaction_id,
        log_type,
        operation_type,
        record_id,
        timestamp,
        CASE 
            WHEN log_type = 'COMMIT' THEN 'SUCCESS'
            WHEN log_type = 'ABORT' THEN 'FAILED'
            ELSE 'IN_PROGRESS'
        END AS status
    FROM transaction_log
    WHERE timestamp > NOW() - INTERVAL hours_back HOUR
    ORDER BY timestamp DESC
    LIMIT limit_count;
END$$

DELIMITER ;

-- ============================================================================
-- 6. COMPARE WITH MAIN
-- ============================================================================
DROP PROCEDURE IF EXISTS compare_with_main;

DELIMITER $$

CREATE PROCEDURE compare_with_main()
BEGIN
    -- Records in Node A but not in Main
    SELECT 
        'IN_NODE_A_NOT_IN_MAIN' AS status,
        a.tconst,
        a.primaryTitle,
        a.startYear
    FROM title_ft a
    LEFT JOIN title_ft_main m ON a.tconst = m.tconst
    WHERE m.tconst IS NULL
    LIMIT 10;
    
    -- Records in Main (for Node A partition) but not in Node A
    SELECT 
        'IN_MAIN_NOT_IN_NODE_A' AS status,
        m.tconst,
        m.primaryTitle,
        m.startYear
    FROM title_ft_main m
    LEFT JOIN title_ft a ON m.tconst = a.tconst
    WHERE m.startYear >= 2025
      AND a.tconst IS NULL
    LIMIT 10;
END$$

DELIMITER ;

SELECT '=== Recovery procedures created on NODE A ===' AS status;
SELECT 'Use sync_from_main() to recover after failures' AS info;
