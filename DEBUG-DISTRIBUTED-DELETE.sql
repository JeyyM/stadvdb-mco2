-- ============================================================================
-- DEBUG DISTRIBUTED DELETE - Test with Node A Down
-- ============================================================================

USE `stadvdb-mco2`;

-- ============================================================================
-- STEP 1: Check if tconst "777" exists on Main only (avoid federated queries)
-- ============================================================================
SELECT '=== STEP 1: Check if 777 exists on Main before delete ===' AS step;

SELECT tconst, primaryTitle, startYear FROM title_ft WHERE tconst = '777';

-- ============================================================================
-- STEP 2: Check transaction log before delete
-- ============================================================================
SELECT '=== STEP 2: Transaction log before delete ===' AS step;
SELECT * FROM transaction_log WHERE record_id = '777' ORDER BY log_sequence ASC;

-- ============================================================================
-- STEP 3: Enable detailed logging for distributed_delete procedure
-- ============================================================================
SELECT '=== STEP 3: About to call distributed_delete ===' AS step;

-- Run the distributed delete
CALL distributed_delete('777');

-- ============================================================================
-- STEP 4: Check if deletion succeeded on Main only
-- ============================================================================
SELECT '=== STEP 4: Check if 777 was deleted from Main ===' AS step;

SELECT COUNT(*) as count_remaining FROM title_ft WHERE tconst = '777';

-- ============================================================================
-- STEP 5: Check transaction log after delete
-- ============================================================================
SELECT '=== STEP 5: Transaction log after delete ===' AS step;
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
WHERE record_id = '777' 
ORDER BY log_sequence ASC;

-- ============================================================================
-- STEP 6: Check what's in the transaction_log for the DELETE transaction
-- ============================================================================
SELECT '=== STEP 6: Full transaction details for 777 delete ===' AS step;

SELECT 
    transaction_id,
    log_sequence,
    log_type,
    timestamp,
    operation_type,
    record_id,
    table_name,
    CASE WHEN new_value IS NOT NULL THEN 'HAS DATA' ELSE 'NULL' END as new_value_status,
    CASE WHEN old_value IS NOT NULL THEN 'HAS DATA' ELSE 'NULL' END as old_value_status,
    source_node
FROM transaction_log 
WHERE record_id = '777'
ORDER BY timestamp DESC, log_sequence DESC
LIMIT 10;

-- ============================================================================
-- STEP 7: Check uncommitted transactions
-- ============================================================================
SELECT '=== STEP 7: Check uncommitted transactions ===' AS step;
CALL check_uncommitted_transactions();

-- ============================================================================
-- STEP 8: Check the recovery checkpoint
-- ============================================================================
SELECT '=== STEP 8: Recovery checkpoint status ===' AS step;
SELECT * FROM recovery_checkpoint;

