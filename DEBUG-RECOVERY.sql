-- ============================================================================
-- DEBUG SCRIPT FOR MAIN RECOVERY
-- Run these queries on Node A to diagnose the issue
-- ============================================================================

USE `stadvdb-mco2-a`;

-- 1. Check what procedures exist
SELECT '=== CHECKING PROCEDURES ===' AS debug_step;
SHOW PROCEDURE STATUS WHERE Db = 'stadvdb-mco2-a' AND Name IN ('full_recovery_main', 'replay_to_main', 'find_missing_on_main');

-- 2. Get the actual procedure definition (first 1000 chars)
SELECT '=== FULL RECOVERY MAIN PROCEDURE CODE ===' AS debug_step;
SELECT SUBSTR(ROUTINE_DEFINITION, 1, 1000) as procedure_code
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = 'stadvdb-mco2-a' 
AND ROUTINE_NAME = 'full_recovery_main'
AND ROUTINE_TYPE = 'PROCEDURE';

-- 3. Check transaction_log table structure
SELECT '=== TRANSACTION LOG TABLE STRUCTURE ===' AS debug_step;
DESCRIBE transaction_log;

-- 4. Check if transaction_log table exists
SELECT '=== CHECKING TABLES ===' AS debug_step;
SHOW TABLES LIKE 'transaction_log%';

-- 5. Count MODIFY rows in transaction_log
SELECT '=== MODIFY ROWS IN TRANSACTION_LOG ===' AS debug_step;
SELECT COUNT(*) as modify_count, MAX(timestamp) as latest_timestamp
FROM transaction_log 
WHERE log_type = 'MODIFY';

-- 6. Show recent MODIFY transactions
SELECT '=== RECENT MODIFY TRANSACTIONS ===' AS debug_step;
SELECT transaction_id, log_sequence, log_type, operation_type, timestamp, node_name
FROM transaction_log 
WHERE log_type = 'MODIFY'
ORDER BY timestamp DESC 
LIMIT 10;

-- 7. Check recovery checkpoint
SELECT '=== RECOVERY CHECKPOINT ===' AS debug_step;
SELECT * FROM recovery_checkpoint WHERE node_name = 'main';

-- 8. Try a simple query from transaction_log
SELECT '=== SIMPLE TEST QUERY ===' AS debug_step;
SELECT 
    operation_type,
    record_id,
    transaction_id,
    timestamp
FROM transaction_log tm
WHERE tm.log_type = 'MODIFY'
  AND tm.timestamp > '2000-01-01 00:00:00.000000'
ORDER BY tm.timestamp ASC, tm.log_sequence ASC
LIMIT 5;

-- 9. Check if title_ft_main federated table exists
SELECT '=== CHECKING FEDERATED TABLES ===' AS debug_step;
SHOW TABLES LIKE 'title_ft%';

-- 10. Test cursor from full_recovery_main
SELECT '=== TEST CURSOR QUERY ===' AS debug_step;
SELECT 
    tm.operation_type,
    tm.new_value,
    tm.old_value,
    tm.record_id,
    tm.transaction_id,
    tm.timestamp
FROM transaction_log tm
WHERE tm.log_type = 'MODIFY'
  AND tm.timestamp > '2000-01-01 00:00:00.000000'
ORDER BY tm.timestamp ASC, tm.log_sequence ASC
LIMIT 5;

-- 11. Check procedure creation time
SELECT '=== PROCEDURES CREATION TIME ===' AS debug_step;
SELECT ROUTINE_NAME, CREATED, LAST_ALTERED
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = 'stadvdb-mco2-a' 
AND ROUTINE_NAME IN ('full_recovery_main', 'replay_to_main', 'find_missing_on_main');

-- 12. Try running recovery with error messages visible
SELECT '=== ATTEMPTING RECOVERY ===' AS debug_step;
SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = 'main';
CALL full_recovery_main(@checkpoint);

-- 13. After recovery, check if checkpoint was updated
SELECT '=== CHECKPOINT AFTER RECOVERY ===' AS debug_step;
SELECT * FROM recovery_checkpoint WHERE node_name = 'main';
WHERE record_id = 'noda' AND log_type = 'MODIFY'
ORDER BY timestamp ASC;

-- Extract and verify startYear from JSON
SELECT 
    transaction_id,
    log_sequence,
    timestamp,
    operation_type,
    new_value,
    JSON_EXTRACT(new_value, '$.startYear') as raw_startYear,
    JSON_UNQUOTE(JSON_EXTRACT(new_value, '$.startYear')) as quoted_startYear,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(new_value, '$.startYear')) AS UNSIGNED) as cast_startYear
FROM transaction_log
WHERE record_id = 'noda' AND log_type = 'MODIFY' AND new_value IS NOT NULL
ORDER BY timestamp ASC;

-- ============================================================================
-- COMPONENT 2: DEBUG RECOVERY CHECKPOINT
-- ============================================================================
SELECT '=== COMPONENT 2: Recovery Checkpoint State ===' AS component;

SELECT * FROM recovery_checkpoint WHERE node_name = 'node_a';

-- ============================================================================
-- COMPONENT 3: DEBUG find_missing_on_node_a QUERY
-- ============================================================================
SELECT '=== COMPONENT 3: find_missing_on_node_a Query Results ===' AS component;
SELECT '--- Testing with checkpoint time 2025-12-04 07:00:00.000000 ---' AS test_params;

SELECT 
    tm.transaction_id,
    tm.timestamp,
    tm.operation_type,
    tm.record_id,
    tm.new_value,
    tm.log_type,
    tm.new_value IS NOT NULL as has_value,
    CASE 
        WHEN tm.new_value IS NULL THEN 'NULL - SKIP'
        ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(tm.new_value, '$.startYear')) AS UNSIGNED)
    END as extracted_startYear
FROM transaction_log tm
WHERE tm.log_type = 'MODIFY'
  AND tm.timestamp > '2025-12-04 07:00:00.000000'
  AND tm.operation_type IS NOT NULL
  AND (
      (tm.operation_type = 'INSERT' AND tm.new_value IS NOT NULL AND CAST(JSON_UNQUOTE(JSON_EXTRACT(tm.new_value, '$.startYear')) AS UNSIGNED) >= 2025)
      OR (tm.operation_type = 'UPDATE' AND tm.new_value IS NOT NULL AND CAST(JSON_UNQUOTE(JSON_EXTRACT(tm.new_value, '$.startYear')) AS UNSIGNED) >= 2025)
      OR (tm.operation_type = 'DELETE' AND tm.old_value IS NOT NULL AND CAST(JSON_UNQUOTE(JSON_EXTRACT(tm.old_value, '$.startYear')) AS UNSIGNED) >= 2025)
  )
ORDER BY tm.timestamp ASC;

-- Count results
SELECT 
    COUNT(*) as total_matching_rows
FROM transaction_log tm
WHERE tm.log_type = 'MODIFY'
  AND tm.timestamp > '2025-12-04 07:00:00.000000'
  AND tm.operation_type IS NOT NULL
  AND (
      (tm.operation_type = 'INSERT' AND tm.new_value IS NOT NULL AND CAST(JSON_UNQUOTE(JSON_EXTRACT(tm.new_value, '$.startYear')) AS UNSIGNED) >= 2025)
      OR (tm.operation_type = 'UPDATE' AND tm.new_value IS NOT NULL AND CAST(JSON_UNQUOTE(JSON_EXTRACT(tm.new_value, '$.startYear')) AS UNSIGNED) >= 2025)
      OR (tm.operation_type = 'DELETE' AND tm.old_value IS NOT NULL AND CAST(JSON_UNQUOTE(JSON_EXTRACT(tm.old_value, '$.startYear')) AS UNSIGNED) >= 2025)
  );

-- ============================================================================
-- COMPONENT 4: DEBUG CURRENT STATE OF NODE A TABLE
-- ============================================================================
SELECT '=== COMPONENT 4: Current State of title_ft_node_a ===' AS component;

-- Check if 'noda' exists on Node A
SELECT 
    tconst,
    primaryTitle,
    runtimeMinutes,
    averageRating,
    numVotes,
    weightedRating,
    startYear
FROM title_ft_node_a
WHERE tconst = 'noda';

-- Count records on Node A
SELECT COUNT(*) as total_records_on_node_a FROM title_ft_node_a;

-- ============================================================================
-- COMPONENT 5: DEBUG WHAT replay_to_node_a WOULD DO
-- ============================================================================
SELECT '=== COMPONENT 5: What replay_to_node_a Would Execute ===' AS component;

-- Get the exact UPDATE statement that would be executed
SELECT 
    CONCAT(
        'UPDATE title_ft_node_a SET ',
        'primaryTitle = "', JSON_UNQUOTE(JSON_EXTRACT(new_value, '$.primaryTitle')), '", ',
        'runtimeMinutes = ', JSON_EXTRACT(new_value, '$.runtimeMinutes'), ', ',
        'averageRating = ', JSON_EXTRACT(new_value, '$.averageRating'), ', ',
        'numVotes = ', JSON_EXTRACT(new_value, '$.numVotes'), ', ',
        'weightedRating = ', JSON_EXTRACT(new_value, '$.weightedRating'), ', ',
        'startYear = ', JSON_EXTRACT(new_value, '$.startYear'), ' ',
        'WHERE tconst = "', JSON_UNQUOTE(JSON_EXTRACT(new_value, '$.tconst')), '";'
    ) as would_execute
FROM transaction_log
WHERE record_id = 'noda' 
  AND log_type = 'MODIFY' 
  AND new_value IS NOT NULL
  AND operation_type = 'UPDATE'
ORDER BY timestamp DESC
LIMIT 1;

