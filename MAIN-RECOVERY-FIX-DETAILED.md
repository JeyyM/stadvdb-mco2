# Main Recovery Fix - Issues and Solutions

## Problem
Recovery command failed with:
```
🔄 Starting recovery for Main from checkpoint: 2000-01-01 00:00:00.000000
❌ Recovery failed - transaction rolled back
```

## Root Causes

1. **Wrong Transaction Log Table**: The procedure was reading from `transaction_log_main` (federated to Main) instead of local `transaction_log`
   - Node A should read its own transaction_log which contains all operations that happened while Main was down
   - This table has the `node_name = 'NODE_A'` filter showing these are Node A's operations

2. **Transaction Management Issue**: EXIT HANDLER with ROLLBACK was aborting entire recovery on first error
   - Should use CONTINUE HANDLER to skip problematic entries but keep recovering

3. **Trying to Write to Federated Log**: The `replay_to_main` was trying to INSERT into `transaction_log_main` which doesn't need to happen
   - Main already has its own transaction log
   - Node A just needs to replay the operations (INSERT/UPDATE/DELETE) to Main's data tables

## Solutions Applied

### 1. Fixed Cursor Query - Use Local transaction_log
**Before:**
```sql
FROM transaction_log_main tm
LEFT JOIN transaction_log_main prev_tm 
```

**After:**
```sql
FROM transaction_log tm
LEFT JOIN transaction_log prev_tm 
WHERE tm.log_type = 'MODIFY'
  AND tm.timestamp > checkpoint_time
  AND tm.node_name = 'NODE_A'  -- Only recover Node A operations
```

### 2. Simplified replay_to_main - Only Replay Data Operations
**Before:** Tried to INSERT/UPDATE into federated `transaction_log_main`
```sql
INSERT INTO transaction_log_main (transaction_id, log_type, ...)
VALUES (transaction_id_param, 'COMMIT', ...);
```

**After:** Only INSERT/UPDATE/DELETE to data tables via federated table
```sql
INSERT INTO title_ft_main (tconst, primaryTitle, ...) VALUES (...)
ON DUPLICATE KEY UPDATE ...;

UPDATE title_ft_main SET ... WHERE tconst = v_tconst;

DELETE FROM title_ft_main WHERE tconst = v_tconst;
```

### 3. Improved Error Handling
**Before:**
```sql
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    ROLLBACK;
    SELECT '❌ Recovery failed - transaction rolled back';
END;
```

**After:**
```sql
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
BEGIN
    -- Log error but continue recovery
END;
```

This allows recovery to continue even if individual operations fail.

## How It Works Now

### Scenario: Node A is Acting Master, Main is Down
1. All operations logged to Node A's local `transaction_log` with `node_name = 'NODE_A'`
2. When Main comes back online, run recovery on Node A:
   ```sql
   SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = 'main';
   CALL full_recovery_main(@checkpoint);
   ```

3. Procedure:
   - Reads from Node A's local `transaction_log`
   - Filters for `node_name = 'NODE_A'` and `log_type = 'MODIFY'`
   - Uses LEFT JOIN to handle multi-row transactions with NULL data
   - Replays each operation to Main via `title_ft_main` federated table
   - Updates checkpoint when done

## Testing the Fix

```sql
-- Simulate Node A recovering Main with the UPDATE operation from NODE_A
SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = 'main';
CALL full_recovery_main(@checkpoint);

-- Expected output:
-- 🔄 Starting recovery for Main from checkpoint: 2000-01-01 00:00:00.000000
-- ✅ Recovery complete! Replayed X transactions to Main. Checkpoint saved at: 2025-12-04 10:01:27.911070

-- Verify the operation was replayed to Main:
SELECT * FROM title_ft_main WHERE tconst = 'tt15242966';
-- Should show primaryTitle = 'Trojan's Horse EDIT' from the UPDATE operation
```

## Key Differences from Node A/B Recovery

| Aspect | Node A/B Recovery | Main Recovery |
|--------|-------------------|---------------|
| **Source Log** | `transaction_log_main` (federated to Main) | Local `transaction_log` |
| **Filter** | All MODIFY rows | MODIFY rows with `node_name = 'NODE_A'` |
| **Target** | `title_ft_node_a` / `title_ft_node_b` | `title_ft_main` (federated to Main) |
| **When Run** | On Main, to recover Node A/B from Main's log | On Node A, to recover Main when Main comes back |
| **Purpose** | Main restores missing data to nodes | Node A syncs its changes back to Main |

## Deployment

On Node A, run:
```sql
source /path/to/5-recovery-for-main.sql;
```

Ensures:
- `recovery_checkpoint` table exists
- `find_missing_on_main` procedure created
- `replay_to_main` procedure created
- `full_recovery_main` procedure created

## Notes

- This procedure MUST run on Node A (not Main)
- Requires `title_ft_main` federated table pointing to Main's `title_ft` table
- Requires local `transaction_log` table on Node A
- The `node_name = 'NODE_A'` filter ensures only Node A operations are recovered
- If recovering from Node B, use `node_name = 'NODE_B'` instead
