-- ============================================================================
-- QUICK DIAGNOSTIC FOR MAIN RECOVERY ISSUE
-- Run on Node A to find the root cause
-- ============================================================================

-- First, verify we're on Node A
SELECT @@hostname as current_server;
SELECT DATABASE() as current_database;

-- Check if procedure still has old code (transaction_log_main reference)
-- This will show the actual procedure definition
SELECT 
    'PROCEDURE DEFINITION' as check_type,
    ROUTINE_DEFINITION as procedure_code
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = 'stadvdb-mco2-a' 
AND ROUTINE_NAME = 'full_recovery_main'
AND ROUTINE_TYPE = 'PROCEDURE'\G

-- If the above shows "transaction_log_main", the old version is still running
-- If it shows "FROM transaction_log tm", the new version is loaded

-- Check what tables exist
SELECT 
    'TABLES' as check_type,
    TABLE_NAME,
    TABLE_SCHEMA
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'stadvdb-mco2-a' 
AND (TABLE_NAME LIKE 'transaction_log%' OR TABLE_NAME = 'recovery_checkpoint');

-- Try to manually run the cursor query that full_recovery_main uses
SELECT 'TESTING CURSOR QUERY' as test;
SET @checkpoint_time = '2000-01-01 00:00:00.000000';

SELECT 
    tm.operation_type,
    tm.record_id,
    tm.transaction_id,
    tm.timestamp,
    tm.new_value
FROM transaction_log tm
WHERE tm.log_type = 'MODIFY'
  AND tm.timestamp > @checkpoint_time
ORDER BY tm.timestamp ASC, tm.log_sequence ASC
LIMIT 5;

-- If the above returns data, the cursor should work
-- If it returns no rows, there's nothing to recover

-- Check recovery checkpoint
SELECT 'RECOVERY CHECKPOINT' as check;
SELECT * FROM recovery_checkpoint WHERE node_name = 'main';

-- Try replay_to_main with hardcoded test data
SELECT 'TESTING REPLAY_TO_MAIN PROCEDURE' as test;
-- This should NOT error if procedure is correct
CALL replay_to_main(
    'UPDATE',
    '{"tconst": "tt15242966", "primaryTitle": "TEST", "runtimeMinutes": 150, "averageRating": 8.1, "numVotes": 11062, "weightedRating": 8.01, "startYear": 2025}',
    '{"tconst": "tt15242966", "primaryTitle": "TEST OLD", "runtimeMinutes": 150, "averageRating": 8.0, "numVotes": 11000, "weightedRating": 7.99, "startYear": 2025}',
    'tt15242966',
    'test-txn-123'
);

-- If the above errors, that's where the problem is
