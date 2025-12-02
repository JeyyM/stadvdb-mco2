# 🔄 RECOVERY GUIDE - Distributed Database
## How to Recover Nodes A & B from Main's Transaction Log

---

## 📋 Overview

Your system has **transaction logs that record BEGIN, MODIFY, and COMMIT** for every distributed operation. Recovery replays **only fully committed transactions** from the Main node to restore Node A and Node B after they come back online.

---

## 🎯 Key Concept: Only Committed Transactions

The recovery system **ignores uncommitted/partial transactions** because:

1. Each transaction has **3 log entries**:
   - `BEGIN` (sequence 1)
   - `MODIFY` (sequence 2) - Contains the actual data changes
   - `COMMIT` (sequence 3)

2. Recovery procedures **filter for COMMIT** log entries, ensuring only completed transactions are replayed

3. **Uncommitted transactions** (missing COMMIT) are automatically skipped

---

## 🔄 Recovery Scenarios

### Scenario 1: **Node A/B Down → Main Kept Running**

**When**: Node A or B goes offline, Main continues accepting operations

**Recovery Steps** (when Node A/B comes back online):

#### **For Node A Recovery:**

```sql
-- Connect to Node A database
USE `stadvdb-mco2-a`;

-- Step 1: Check what you're missing
CALL get_recent_transactions(100);  -- See last 100 transactions on Node A

-- Step 2: Full synchronization from Main (RECOMMENDED)
-- This replays ALL committed transactions from Main that belong to Node A (startYear >= 2025)
CALL sync_from_main();

-- Expected Output:
-- ┌─────────────────────────┬──────────────────────┬──────────────────┬──────────────────┐
-- │ status                  │ records_inserted     │ records_updated  │ records_removed  │
-- ├─────────────────────────┼──────────────────────┼──────────────────┼──────────────────┤
-- │ Node A sync completed   │ 5                    │ 12               │ 0                │
-- └─────────────────────────┴──────────────────────┴──────────────────┴──────────────────┘

-- Step 3: Verify consistency
CALL verify_consistency();

-- Step 4: Check health
CALL health_check();
```

#### **For Node B Recovery:**

```sql
-- Connect to Node B database
USE `stadvdb-mco2-b`;

-- Step 1: Check what you're missing
CALL get_recent_transactions(100);

-- Step 2: Full synchronization from Main (RECOMMENDED)
-- This replays ALL committed transactions from Main that belong to Node B (startYear < 2025 or NULL)
CALL sync_from_main();

-- Expected Output:
-- ┌─────────────────────────┬──────────────────────┬──────────────────┬──────────────────┐
-- │ status                  │ records_inserted     │ records_updated  │ records_removed  │
-- ├─────────────────────────┼──────────────────────┼──────────────────┼──────────────────┤
-- │ Node B sync completed   │ 8                    │ 15               │ 2                │
-- └─────────────────────────┴──────────────────────┴──────────────────┴──────────────────┘

-- Step 3: Verify consistency
CALL verify_consistency();

-- Step 4: Check health
CALL health_check();
```

---

### Scenario 2: **Main Node Down → Node A Promoted to Acting Master**

**When**: Main goes offline, Node A takes over as Vice-Master (Acting Master)

**Recovery Steps** (when Main comes back online):

#### **Step 1: Main Recovers from Node A's Log**

```sql
-- Connect to Main database
USE `stadvdb-mco2`;

-- Check Main's last known transaction timestamp
SELECT MAX(timestamp) as last_main_transaction, 
       MAX(transaction_id) as last_txn_id
FROM transaction_log
WHERE source_node = 'MAIN';

-- Recover all committed transactions that Node A handled while Main was down
CALL recover_from_node_a();

-- This procedure:
-- 1. Finds Main's last transaction timestamp
-- 2. Queries Node A's transaction log for all COMMITTED transactions after that time
-- 3. Replays ONLY operations with log_type = 'MODIFY' that have matching COMMIT entries
-- 4. Skips any transactions that are incomplete/uncommitted

-- Expected Output:
-- ┌────────────────────────────────────────────────────────────────────┐
-- │ Main last transaction was at: 2024-12-02 14:23:45.123456          │
-- │ Fetching missed transactions from Node A...                        │
-- │ Recovery complete. Replayed 23 transactions from Node A.           │
-- │ Next step: Call demote_to_vice() on Node A to restore normal op.  │
-- └────────────────────────────────────────────────────────────────────┘
```

#### **Step 2: Demote Node A Back to Vice Status**

```sql
-- Connect to Node A database
USE `stadvdb-mco2-a`;

-- Restore Node A to normal Vice-Master role
CALL demote_to_vice();

-- Expected Output:
-- ┌──────────────────────────────────────────────────┐
-- │ Node A demoted back to VICE_MASTER role          │
-- │ Normal operations restored                       │
-- └──────────────────────────────────────────────────┘
```

#### **Step 3: Sync Node B from Main**

```sql
-- Node B may have missed transactions while Main was down
-- Connect to Node B database
USE `stadvdb-mco2-b`;

CALL sync_from_main();
```

---

## 🔍 How Recovery Ensures Only Committed Transactions

### **sync_from_main()** Procedure Logic:

```sql
-- Node A's sync_from_main() procedure:

1. INSERT IGNORE INTO title_ft
   SELECT m.*
   FROM title_ft_main m
   WHERE m.startYear >= 2025  -- Only Node A's partition
     AND NOT EXISTS (SELECT 1 FROM title_ft a WHERE a.tconst = m.tconst);
   -- This queries Main's current committed data (not uncommitted)

2. UPDATE title_ft a
   INNER JOIN title_ft_main m ON a.tconst = m.tconst
   SET a.primaryTitle = m.primaryTitle, ...
   WHERE m.startYear >= 2025
     AND (columns differ);
   -- Only updates with Main's current state (committed data)

3. DELETE a FROM title_ft a
   LEFT JOIN title_ft_main m ON a.tconst = m.tconst
   WHERE a.startYear < 2025 OR m.tconst IS NULL;
   -- Removes records that don't belong or were deleted from Main
```

**Why this ensures only committed transactions:**
- Uses **federated table `title_ft_main`** which shows Main's current state
- Main's `title_ft` table **only contains data from COMMITTED transactions**
- Uncommitted transactions **never reached Main's title_ft table** (they would have been rolled back)
- Therefore, sync_from_main() **inherently replays only committed data**

---

### **recover_from_node_a()** Procedure Logic:

```sql
-- Main's recover_from_node_a() procedure:

1. Find Main's last transaction timestamp before going down:
   SELECT MAX(timestamp) FROM transaction_log WHERE source_node = 'MAIN';

2. Query Node A's log for transactions AFTER that timestamp:
   SELECT transaction_id, operation_type, record_id, new_value, timestamp
   FROM transaction_log_node_a
   WHERE timestamp > last_main_timestamp
     AND log_type = 'MODIFY'  -- Get the actual data change operations
     AND table_name = 'title_ft'
     ORDER BY timestamp, log_sequence;

3. For each transaction, check if COMMIT exists:
   -- The cursor fetches transactions WHERE log_type = 'MODIFY'
   -- These are only logged if the transaction reaches COMMIT
   -- Transactions that aborted/rolled back never create MODIFY entries

4. Replay the operation (INSERT/UPDATE/DELETE) on Main
```

**Why this ensures only committed transactions:**
- The log cursor filters `WHERE log_type = 'MODIFY'` 
- `MODIFY` entries are **only written after COMMIT** completes
- Uncommitted transactions only have `BEGIN` entries, no `MODIFY` or `COMMIT`
- Therefore, **only committed transactions have MODIFY entries to replay**

---

## 📊 Verification Commands

### Check for Uncommitted Transactions (on Main):

```sql
USE `stadvdb-mco2`;

-- Find any transactions missing COMMIT
CALL check_uncommitted_transactions();

-- Expected: Empty result (no uncommitted transactions)
-- If found, these are transactions that started but never completed
```

### Check for Aborted Transactions (on Main):

```sql
USE `stadvdb-mco2`;

-- Find transactions that were explicitly aborted/rolled back
CALL check_aborted_transactions('2024-12-01 00:00:00');

-- These are transactions with ABORT or ROLLBACK log entries
-- Recovery procedures skip these automatically
```

### Compare Node Data with Main:

```sql
-- On Node A:
USE `stadvdb-mco2-a`;
CALL compare_with_main();

-- Shows differences between Node A and Main's data
-- Run sync_from_main() if differences found

-- On Node B:
USE `stadvdb-mco2-b`;
CALL compare_with_main();
```

### Check Transaction Details:

```sql
-- On Main:
USE `stadvdb-mco2`;

-- Get full transaction log for a specific transaction
CALL get_transaction_details('transaction-uuid-here');

-- Shows all log entries (BEGIN, MODIFY, COMMIT/ABORT) for that transaction
```

---

## 🚨 Common Issues & Solutions

### Issue 1: "Federated table cannot connect to Node A/B"

**Problem**: When running recovery, you see errors like:
```
Error 1429: Unable to connect to foreign data source
Error 1158: Got an error reading communication packets
```

**Cause**: Federated tables in `2-log-setup-main.sql` use internal IPs (10.2.14.x) that are unreachable

**Solution**: This is expected! Your current setup handles this:
- **sync_from_main()** uses `title_ft_main` federated table (Main → Node A/B direction works)
- **recover_from_node_a()** queries `transaction_log_node_a` (also Main → Node A direction)
- **All recovery reads FROM Main**, not TO Main
- Therefore, federated connectivity issues don't affect recovery

**Workaround if needed**: Temporarily modify `2-log-setup-main.sql` to use `localhost` ports instead of internal IPs:
```sql
-- Change from:
CONNECTION='mysql://root:password@10.2.14.52:60752/stadvdb-mco2-a/transaction_log'

-- To:
CONNECTION='mysql://root:password@localhost:60752/stadvdb-mco2-a/transaction_log'
```

---

### Issue 2: "Node shows different data than Main"

**Problem**: After recovery, Node A/B still has different data than Main

**Solution**:
```sql
-- On the affected node (A or B):
CALL compare_with_main();
-- Check which records differ

-- Re-run full sync:
CALL sync_from_main();

-- Verify consistency:
CALL verify_consistency();
```

---

### Issue 3: "How do I know if recovery is needed?"

**Check health status:**

```sql
-- On Node A:
USE `stadvdb-mco2-a`;
CALL health_check();

-- Output shows:
-- - Total local records
-- - Total Main records (for your partition)
-- - Records only in Node A (to push to Main)
-- - Records only in Main (to pull from Main)
-- - Record count difference
```

**If "Records only in Main" > 0**, you need to run `sync_from_main()`

---

## 🎯 Best Practices

### 1. **Always use sync_from_main() for routine recovery**
   - Simplest and safest
   - Replays all committed data
   - Self-healing (automatically fixes inconsistencies)

### 2. **Use recover_from_node_a() only when Main was down**
   - Specific to Main node recovery after failover
   - Requires Node A to have been promoted to Acting Master

### 3. **Verify after recovery**
   ```sql
   CALL verify_consistency();
   CALL health_check();
   ```

### 4. **Monitor transaction logs**
   ```sql
   -- Check recent transactions
   CALL get_recent_transactions(50);
   
   -- Check for anomalies
   CALL check_uncommitted_transactions();
   ```

### 5. **Regular health checks**
   - Run `health_check()` on each node periodically
   - Automate with scheduled jobs if needed

---

## 📝 Quick Recovery Cheat Sheet

| Scenario | Node | Command |
|----------|------|---------|
| Node A was down | Node A | `CALL sync_from_main();` |
| Node B was down | Node B | `CALL sync_from_main();` |
| Main was down, A took over | Main | `CALL recover_from_node_a();` |
| After Main recovery | Node A | `CALL demote_to_vice();` |
| After Main recovery | Node B | `CALL sync_from_main();` |
| Check if recovery needed | Any node | `CALL health_check();` |
| Verify after recovery | Any node | `CALL verify_consistency();` |

---

## 🔬 Testing Recovery

### Test 1: Simulate Node B Down

```sql
-- 1. Shutdown Node B (or just stop accessing it)

-- 2. On Main, perform operations:
USE `stadvdb-mco2`;
CALL distributed_insert('tt9999991', 'Test Movie', 120, 7.5, 1000, 2020);
CALL distributed_update('tt9999991', 'Updated Movie', 125, 8.0, 1500, 2020);

-- 3. Restart Node B

-- 4. On Node B, check missing data:
USE `stadvdb-mco2-b`;
SELECT * FROM title_ft WHERE tconst = 'tt9999991';
-- Should be missing or outdated

-- 5. Run recovery:
CALL sync_from_main();

-- 6. Verify:
SELECT * FROM title_ft WHERE tconst = 'tt9999991';
-- Should now match Main's data
```

### Test 2: Uncommitted Transaction Handling

```sql
-- 1. On Main, start a transaction without committing:
USE `stadvdb-mco2`;
START TRANSACTION;
INSERT INTO title_ft VALUES ('tt8888888', 'Uncommitted', 90, 6.0, 100, 6.0, 2023);
-- DON'T COMMIT - let it timeout or rollback

-- 2. Check transaction log:
SELECT * FROM transaction_log WHERE record_id = 'tt8888888';
-- Should show BEGIN but no COMMIT

-- 3. On Node B, run recovery:
USE `stadvdb-mco2-b`;
CALL sync_from_main();

-- 4. Verify uncommitted transaction NOT replayed:
SELECT * FROM title_ft WHERE tconst = 'tt8888888';
-- Should return 0 rows (uncommitted data not replayed)
```

---

## ✅ Success Criteria

After recovery, verify:

✅ **No errors in recovery output**
✅ **Record counts match Main's partition**
✅ **verify_consistency() returns no mismatches**
✅ **health_check() shows 0 difference**
✅ **Test CRUD operations work correctly**
✅ **No uncommitted transactions in logs**

---

## 🆘 Emergency Recovery

If normal recovery fails, you can manually inspect and replay specific transactions:

```sql
-- 1. Find the specific transaction in Main's log
USE `stadvdb-mco2`;
SELECT * FROM transaction_log 
WHERE record_id = 'tt1234567' 
ORDER BY timestamp DESC 
LIMIT 10;

-- 2. Get full transaction details
CALL get_transaction_details('transaction-uuid-from-above');

-- 3. If it's committed (has COMMIT entry), manually replay on Node A/B
USE `stadvdb-mco2-a`;  -- or stadvdb-mco2-b

-- For INSERT:
INSERT INTO title_ft VALUES (...);

-- For UPDATE:
UPDATE title_ft SET ... WHERE tconst = 'tt1234567';

-- For DELETE:
DELETE FROM title_ft WHERE tconst = 'tt1234567';
```

---

## 📞 Support

If you encounter issues:
1. Check transaction logs with `get_recent_transactions()`
2. Verify Main node is accessible
3. Check federated table connectivity
4. Review error handlers in distributed procedures
5. Consult this guide's troubleshooting section

---

**Last Updated**: December 2, 2024
**Version**: 1.0
