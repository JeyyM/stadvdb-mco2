# CRITICAL FIX: Remove transaction_log_main Reference

## The Problem
The recovery was freezing because it was querying `transaction_log_main` (federated table to Main) which was timing out:

```
Error Code: 1429. Unable to connect to foreign data source: 
Can't connect to MySQL server on 'ccscloud.dlsu.edu.ph:60751' (timeout)
```

## The Solution
Changed ALL queries from `transaction_log_main` to LOCAL `transaction_log` table.

### What Changed

**Before (WRONG):**
```sql
-- find_missing_on_main procedure
FROM transaction_log_main tm
LEFT JOIN transaction_log_main prev_tm

-- full_recovery_main cursor
FROM transaction_log_main tm
LEFT JOIN transaction_log_main prev_tm
```

**After (CORRECT):**
```sql
-- find_missing_on_main procedure
FROM transaction_log tm
LEFT JOIN transaction_log prev_tm

-- full_recovery_main cursor
FROM transaction_log tm
LEFT JOIN transaction_log prev_tm
```

## Why This Works

**Scenario: Main is down, Node A is acting master**
1. All operations on Node A are logged to Node A's LOCAL `transaction_log` table
2. When Main comes back online, Node A reads its LOCAL `transaction_log` (no network call)
3. Node A replays operations to Main via `title_ft_main` federated table (one-way call)

**What was wrong:**
- Was trying to READ from Main's transaction_log via federated table
- That required a connection TO Main, which was down
- Caused timeout/freeze

## Deployment

**IMMEDIATELY on Node A:**
```sql
source /path/to/Modifications/5-recovery-for-main.sql;
```

This will:
- DROP old procedures (with transaction_log_main references)
- CREATE new procedures (using local transaction_log)
- No more timeouts

## Testing After Deployment

```sql
-- Check procedures were updated
SHOW PROCEDURE STATUS WHERE Db = 'stadvdb-mco2-a' AND Name LIKE 'full_recovery%';

-- Run recovery (should NOT freeze now)
SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = 'main';
CALL full_recovery_main(@checkpoint);

-- Should see:
-- 🔄 Starting recovery for Main from checkpoint: ...
-- 📊 Found X MODIFY transactions to process
-- ✅ Recovery complete! Replayed X transactions to Main.
```

## Key Insight

When recovering TO Main from Node A:
- ✅ READ from: Node A's local `transaction_log`
- ✅ WRITE to: Main's `title_ft_main` (federated)
- ❌ DON'T READ from: Main's `transaction_log_main` (federated) - causes timeout

The direction matters! Reading from federated tables that require connecting to a down server = timeout. Writing to federated tables is one-way and works fine even if connection is slower.
