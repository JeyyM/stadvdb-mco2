# Recovery for Main - Fixes Applied

## Overview
Fixed `5-recovery-for-main.sql` to properly handle recovery of Main database when Node A acts as the temporary master. This file is used when Main goes down and we need to recover it from Node A's transaction logs.

## Changes Applied

### 1. **Updated `find_missing_on_main` Procedure**

**Before:** Queried `log_type = 'COMMIT'` (which always have NULL data)
```sql
WHERE tm.log_type = 'COMMIT'
  AND tm.timestamp > checkpoint_time
  AND tm.operation_type IS NOT NULL
```

**After:** Uses LEFT JOIN pattern to read from MODIFY rows and handle NULL data
```sql
FROM transaction_log_main tm
LEFT JOIN transaction_log_main prev_tm 
    ON tm.transaction_id = prev_tm.transaction_id 
    AND tm.log_sequence = prev_tm.log_sequence + 1
    AND prev_tm.new_value IS NOT NULL
WHERE tm.log_type = 'MODIFY'
  AND tm.timestamp > checkpoint_time
```

### 2. **Updated `replay_to_main` Procedure**

**Changes:**
- Added parameters: `record_id_param` and `transaction_id_param`
- Added federated error handling with CONTINUE HANDLER for error codes: 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
- Implemented **Two-Phase Commit Pattern**:
  - **Phase 1**: Insert/Update/Delete on Main table + log entry (guaranteed to Main's own database)
  - **Phase 2**: Could be used for federated replication if needed (but in this context, we're recovering TO Main)

**Key Pattern for INSERT:**
```sql
START TRANSACTION;
IF NOT EXISTS (SELECT 1 FROM title_ft WHERE tconst = v_tconst) THEN
    INSERT INTO title_ft (tconst, primaryTitle, ...) VALUES (...);
    INSERT INTO transaction_log_main (...) VALUES (...);
    COMMIT;
ELSE
    ROLLBACK;
END IF;
```

### 3. **Updated `full_recovery_main` Procedure**

**Major Changes:**

1. **Cursor Query Fix**: Changed from COMMIT to MODIFY rows with LEFT JOIN
   - Reads `tm.log_type = 'MODIFY'` instead of `'COMMIT'`
   - Uses LEFT JOIN to previous row to get data when current row is NULL
   - Includes `log_sequence ASC` in ORDER BY for proper multi-row transaction ordering

2. **Removed Outer Transaction Block**: 
   - Removed `START TRANSACTION` before cursor loop
   - Each `replay_to_main` call now handles its own transaction (more granular)
   - Prevents massive rollback if single operation fails

3. **Updated Variable Names**:
   - Changed `v_tconst` to `v_record_id` (more generic)
   - Added `v_record_id` and `v_transaction_id` to pass to replay procedure

4. **Simplified Error Handling**:
   - EXIT HANDLER only triggers on SQL exceptions
   - Individual replay operations won't crash entire recovery

**Before (Problematic):**
```sql
START TRANSACTION;  -- outer transaction
SET v_max_timestamp = checkpoint_time;
BEGIN
    DECLARE cur CURSOR FOR
        SELECT ... FROM transaction_log_main tm
        WHERE tm.log_type = 'COMMIT'  -- ❌ WRONG: COMMIT rows have NULL data
        AND tm.timestamp > checkpoint_time
        AND tm.operation_type IS NOT NULL
        ORDER BY tm.timestamp ASC;
```

**After (Fixed):**
```sql
SET v_max_timestamp = checkpoint_time;
BEGIN
    DECLARE cur CURSOR FOR
        SELECT 
            COALESCE(tm.operation_type, prev_tm.operation_type) AS operation_type,
            ...
        FROM transaction_log_main tm
        LEFT JOIN transaction_log_main prev_tm 
            ON tm.transaction_id = prev_tm.transaction_id 
            AND tm.log_sequence = prev_tm.log_sequence + 1
            AND prev_tm.new_value IS NOT NULL
        WHERE tm.log_type = 'MODIFY'  -- ✅ CORRECT: MODIFY rows have actual data
          AND tm.timestamp > checkpoint_time
        ORDER BY COALESCE(tm.timestamp, prev_tm.timestamp) ASC, tm.log_sequence ASC;
```

## How It Works

### Scenario: Main is Down, Node A is Acting Master

1. **Operations on Node A**: All CRUD operations run on Node A, logged to Node A's transaction_log
2. **Federated Replication**: Node A tries to replicate to Main (but fails - Main is down)
3. **Main Comes Back Online**: 
   - Node A queries its own transaction log
   - Sends MODIFY rows to Main via `full_recovery_main` procedure
   - Multi-row transactions are handled correctly via LEFT JOIN
   - Each operation is committed to Main individually (transactional safety)

### Key Improvements

| Aspect | Before | After |
|--------|--------|-------|
| **Log Entry Type** | COMMIT rows (always NULL data) | MODIFY rows (actual operation data) |
| **Multi-Row Handling** | ❌ Skipped rows with NULL data | ✅ LEFT JOIN to previous row |
| **Transaction Safety** | ❌ Outer transaction (all-or-nothing) | ✅ Individual commits (granular) |
| **Error Handling** | ❌ Single error crashes recovery | ✅ Errors logged, recovery continues |
| **Federated Errors** | ❌ Not handled | ✅ CONTINUE HANDLER for 1429, 1296, etc. |

## Testing

To test Main recovery after Node A acts as master:

```sql
-- 1. Verify checkpoint
SELECT * FROM recovery_checkpoint WHERE node_name = 'main';

-- 2. Run recovery (assuming Node A has the transaction log)
CALL full_recovery_main(NOW() - INTERVAL 1 HOUR);

-- 3. Verify recovery checkpoint was updated
SELECT * FROM recovery_checkpoint WHERE node_name = 'main';

-- 4. Verify data was replayed (compare Main with Node A)
SELECT COUNT(*) FROM title_ft;
```

## File Dependencies

- **Uses**: `transaction_log_main` (federated table pointing to Main's transaction log)
- **Updates**: `recovery_checkpoint` table
- **Updates**: `title_ft` table (Main's data)

## Notes

- This procedure runs on Node A when Main is being recovered
- Queries are federated to Main's transaction_log
- Each operation commits individually for data safety
- Checkpoint system ensures no duplicate recovery
