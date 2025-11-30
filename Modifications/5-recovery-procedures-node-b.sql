-- ============================================================================
-- RECOVERY PROCEDURES FOR NODE B
-- These procedures help Node B recover and synchronize with Main
-- ============================================================================

USE `stadvdb-mco2-b`;

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
    
    -- Insert missing records from Main that belong to Node B (startYear < 2025 or NULL)
    INSERT IGNORE INTO title_ft
    SELECT m.*
    FROM title_ft_main m
    WHERE (m.startYear < 2025 OR m.startYear IS NULL)
      AND NOT EXISTS (
        SELECT 1 FROM title_ft b
        WHERE b.tconst = m.tconst
      );
    
    SET synced_count = ROW_COUNT();
    
    -- Update records that differ from Main
    UPDATE title_ft b
    INNER JOIN title_ft_main m ON b.tconst = m.tconst
    SET b.primaryTitle = m.primaryTitle,
        b.runtimeMinutes = m.runtimeMinutes,
        b.averageRating = m.averageRating,
        b.numVotes = m.numVotes,
        b.weightedRating = m.weightedRating,
        b.startYear = m.startYear
    WHERE (m.startYear < 2025 OR m.startYear IS NULL)
      AND (b.primaryTitle != m.primaryTitle
        OR b.runtimeMinutes != m.runtimeMinutes
        OR b.numVotes != m.numVotes
        OR b.averageRating != m.averageRating
        OR b.weightedRating != m.weightedRating
        OR b.startYear != m.startYear
        OR (b.startYear IS NULL AND m.startYear IS NOT NULL)
        OR (b.startYear IS NOT NULL AND m.startYear IS NULL));
    
    SET updated_count = ROW_COUNT();
    
    -- Remove records that don't belong to Node B anymore
    DELETE b FROM title_ft b
    LEFT JOIN title_ft_main m ON b.tconst = m.tconst
    WHERE b.startYear >= 2025
       OR m.tconst IS NULL;  -- Record deleted from Main
    
    SET deleted_count = ROW_COUNT();
    
    COMMIT;
    
    SELECT 
        'Node B sync completed' AS status,
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
    -- Find committed transactions in Node B that aren't in Main's log
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
    -- Check for records in Node B that don't match Main
    SELECT 
        'MISMATCH' AS status,
        b.tconst,
        b.primaryTitle AS node_b_title,
        m.primaryTitle AS main_title,
        b.numVotes AS node_b_votes,
        m.numVotes AS main_votes,
        b.averageRating AS node_b_rating,
        m.averageRating AS main_rating
    FROM title_ft b
    INNER JOIN title_ft_main m ON b.tconst = m.tconst
    WHERE (b.startYear < 2025 OR b.startYear IS NULL)
      AND (b.primaryTitle != m.primaryTitle
        OR b.numVotes != m.numVotes
        OR b.averageRating != m.averageRating
        OR b.weightedRating != m.weightedRating)
    LIMIT 100;
    
    -- Summary counts
    SELECT 
        (SELECT COUNT(*) FROM title_ft) AS node_b_count,
        (SELECT COUNT(*) FROM title_ft_main WHERE startYear < 2025 OR startYear IS NULL) AS main_node_b_partition,
        (SELECT COUNT(*) FROM title_ft WHERE startYear >= 2025) AS wrong_partition;
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
        COUNT(CASE WHEN startYear < 2025 OR startYear IS NULL THEN 1 END) AS correct_partition,
        COUNT(CASE WHEN startYear >= 2025 THEN 1 END) AS wrong_partition,
        MIN(startYear) AS min_year,
        MAX(startYear) AS max_year,
        COUNT(CASE WHEN startYear IS NULL THEN 1 END) AS null_years
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
    -- Records in Node B but not in Main
    SELECT 
        'IN_NODE_B_NOT_IN_MAIN' AS status,
        b.tconst,
        b.primaryTitle,
        b.startYear
    FROM title_ft b
    LEFT JOIN title_ft_main m ON b.tconst = m.tconst
    WHERE m.tconst IS NULL
    LIMIT 10;
    
    -- Records in Main (for Node B partition) but not in Node B
    SELECT 
        'IN_MAIN_NOT_IN_NODE_B' AS status,
        m.tconst,
        m.primaryTitle,
        m.startYear
    FROM title_ft_main m
    LEFT JOIN title_ft b ON m.tconst = b.tconst
    WHERE (m.startYear < 2025 OR m.startYear IS NULL)
      AND b.tconst IS NULL
    LIMIT 10;
END$$

DELIMITER ;

SELECT '=== Recovery procedures created on NODE B ===' AS status;
SELECT 'Use sync_from_main() to recover after failures' AS info;
