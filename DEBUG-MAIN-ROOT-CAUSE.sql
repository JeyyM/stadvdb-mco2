 -- ============================================================================
-- MAIN RECOVERY - ROOT CAUSE ANALYSIS
-- Run these checks in order on Node A
-- ============================================================================

USE `stadvdb-mco2-a`;

-- ============================================================================
-- STEP 1: Verify Node A is online and connected
-- ============================================================================
SELECT '=== STEP 1: NODE A STATUS ===' AS step;
SELECT @@hostname, @@version, DATABASE();

-- ============================================================================
-- STEP 2: Check if full_recovery_main procedure has OLD CODE
-- ============================================================================
SELECT '=== STEP 2: CHECK IF PROCEDURE WAS UPDATED ===' AS step;

-- Look for "transaction_log_main" in the procedure (OLD code)
SELECT 
    CASE 
        WHEN ROUTINE_DEFINITION LIKE '%transaction_log_main%' THEN 'USING transaction_log_main (OLD CODE - NEEDS REDEPLOYMENT)'
        WHEN ROUTINE_DEFINITION LIKE '%FROM transaction_log tm%' THEN 'USING transaction_log (NEW CODE - CORRECT)'
        ELSE 'UNKNOWN CODE'
    END as procedure_status
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = 'stadvdb-mco2-a' 
AND ROUTINE_NAME = 'full_recovery_main';

-- ============================================================================
-- STEP 3: Check if title_ft_main federated table exists
-- ============================================================================
SELECT '=== STEP 3: CHECK title_ft_main TABLE ===' AS step;

-- This query should return 1 row if table exists
SELECT COUNT(*) as title_ft_main_exists
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'stadvdb-mco2-a' 
AND TABLE_NAME = 'title_ft_main';

-- If it doesn't exist, that's the problem!
-- Run this to create it:
-- CREATE TABLE IF NOT EXISTS title_ft_main (
--     tconst VARCHAR(12) PRIMARY KEY,
--     primaryTitle VARCHAR(1024),
--     runtimeMinutes SMALLINT UNSIGNED,
--     averageRating DECIMAL(3,1),
--     numVotes INT UNSIGNED,
--     weightedRating DECIMAL(4,2),
--     startYear SMALLINT UNSIGNED
-- ) ENGINE=FEDERATED
-- CONNECTION='mysql://root:password@10.2.14.51:3306/stadvdb-mco2/title_ft';

-- ============================================================================
-- STEP 4: Test if we can write to title_ft_main
-- ============================================================================
SELECT '=== STEP 4: TEST title_ft_main ACCESS (might timeout) ===' AS step;

-- Try a simple write (with error handler to avoid timeout)
BEGIN
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
    BEGIN
        SELECT 'title_ft_main is UNREACHABLE - this is expected if Main is down' as warning;
    END;
    
    INSERT INTO title_ft_main (tconst, primaryTitle, startYear) 
    VALUES ('tt-test-debug', 'Debug Test', 2025)
    ON DUPLICATE KEY UPDATE primaryTitle = 'Debug Test Updated';
    
    SELECT 'title_ft_main is REACHABLE and working' as success;
END;

-- ============================================================================
-- STEP 5: Check transaction_log has data to recover
-- ============================================================================
SELECT '=== STEP 5: CHECK TRANSACTION_LOG DATA ===' AS step;

SELECT 
    COUNT(*) as total_modify_rows,
    MAX(timestamp) as latest_transaction,
    MIN(timestamp) as earliest_transaction
FROM transaction_log 
WHERE log_type = 'MODIFY';

-- Show sample of MODIFY rows
SELECT 'Sample MODIFY transactions:' as info;
SELECT 
    transaction_id,
    operation_type,
    record_id,
    timestamp,
    new_value
FROM transaction_log 
WHERE log_type = 'MODIFY'
ORDER BY timestamp DESC
LIMIT 5;

-- ============================================================================
-- STEP 6: Check if replay_to_main procedure works at all
-- ============================================================================
SELECT '=== STEP 6: TEST replay_to_main PROCEDURE ===' AS step;

-- This test should work even if title_ft_main is unreachable
-- The federated error will be caught silently
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT '⚠️ replay_to_main errored (likely title_ft_main unreachable)' as error_info;
    END;
    
    CALL replay_to_main(
        'UPDATE',
        '{"tconst": "tt-test", "primaryTitle": "Test", "runtimeMinutes": 100, "averageRating": 7.5, "numVotes": 1000, "weightedRating": 7.0, "startYear": 2025}',
        '{"tconst": "tt-test"}',
        'tt-test',
        'test-txn'
    );
    
    SELECT '✅ replay_to_main executed' as success;
END;

-- ============================================================================
-- STEP 7: Run full recovery with detailed output
-- ============================================================================
SELECT '=== STEP 7: ATTEMPT FULL RECOVERY ===' AS step;

SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = 'main';

CALL full_recovery_main(@checkpoint);

-- ============================================================================
-- STEP 8: Check final status
-- ============================================================================
SELECT '=== STEP 8: FINAL STATUS ===' AS step;

SELECT * FROM recovery_checkpoint WHERE node_name = 'main';
