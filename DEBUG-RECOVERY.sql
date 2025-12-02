-- ============================================================================
-- SYSTEMATIC RECOVERY PROCEDURE DEBUGGING
-- Test each component individually
-- ============================================================================

USE `stadvdb-mco2`;

-- ============================================================================
-- COMPONENT 1: DEBUG TRANSACTION_LOG DATA
-- ============================================================================
SELECT '=== COMPONENT 1: Transaction Log Inspection ===' AS component;

-- Check the 'noda' transaction exists
SELECT 
    transaction_id,
    log_sequence,
    log_type,
    timestamp,
    operation_type,
    record_id,
    table_name,
    new_value,
    old_value,
    source_node
FROM transaction_log
WHERE record_id = 'noda'
ORDER BY timestamp ASC, log_sequence ASC;

-- Count MODIFY vs COMMIT rows for 'noda'
SELECT 
    log_type,
    COUNT(*) as count,
    MIN(timestamp) as earliest,
    MAX(timestamp) as latest
FROM transaction_log
WHERE record_id = 'noda'
GROUP BY log_type;

-- Check for NULL values in new_value
SELECT 
    transaction_id,
    log_sequence,
    log_type,
    timestamp,
    operation_type,
    new_value IS NULL as new_value_is_null,
    old_value IS NULL as old_value_is_null,
    new_value,
    old_value
FROM transaction_log
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

