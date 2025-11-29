# Write-Ahead Logging (WAL) and Recovery Guide

## Overview
This distributed database implements a comprehensive WAL system that enables recovery across all nodes. Each node maintains its own transaction log for both local and federated operations.

## How It Works

### 1. Main Node Operations
When a client performs an INSERT/UPDATE/DELETE through the Main node:

**Main Node Logs:**
```
log_sequence: 1, log_type: BEGIN, source_node: MAIN
log_sequence: 2, log_type: MODIFY, source_node: MAIN (change to local title_ft)
log_sequence: 3, log_type: COMMIT, source_node: MAIN
```

**Node A/B Logs (for federated replication):**
```
log_sequence: 1, log_type: MODIFY, source_node: MAIN (replicated change)
```

### 2. Direct Node Operations (when Main is down)
If you connect directly to Node A or Node B:

**Node A/B Logs:**
```
log_sequence: 1, log_type: BEGIN, source_node: NODE_A (or NODE_B)
log_sequence: 2, log_type: MODIFY, source_node: NODE_A (or NODE_B)
log_sequence: 3, log_type: COMMIT, source_node: NODE_A (or NODE_B)
```

## Log Structure

### Complete Transaction (Main Node)
```sql
transaction_id | log_sequence | log_type | source_node | table_name | operation_type
UUID-123       | 1            | BEGIN    | MAIN        | NULL       | NULL
UUID-123       | 2            | MODIFY   | MAIN        | title_ft   | UPDATE
UUID-123       | 3            | COMMIT   | MAIN        | NULL       | NULL
```

### Replicated Entry (Node A/B)
```sql
transaction_id | log_sequence | log_type | source_node | table_name | operation_type
UUID-123       | 1            | MODIFY   | MAIN        | title_ft   | UPDATE
```

**Key Difference:**
- `source_node: MAIN` = This was replicated from Main node
- `source_node: NODE_A/B` = This was a direct operation on this node
- No BEGIN/COMMIT for replicated entries (Main already logged those)

## Recovery Scenarios

### Case 1: Main Node Fails During Replication
**Scenario:** Node 2/3 successfully receives the write, but Main crashes before COMMIT

**Main Node Log:**
```
BEGIN → MODIFY → (crash, no COMMIT)
```

**Node A Log:**
```
MODIFY (source: MAIN, transaction_id: UUID-123)
```

**Recovery:**
1. Main comes back online
2. Query transaction_log for transactions with BEGIN but no COMMIT/ABORT
3. Check Node A/B logs for same transaction_id
4. If found in Node A/B → COMMIT the transaction on Main
5. If not found → ROLLBACK on Main

### Case 2: Main Eventually Recovers, Missed Transactions
**Scenario:** Main was down, users connected directly to Node A/B

**Node A Log:**
```
transaction_id: UUID-456
BEGIN (source: NODE_A) → MODIFY → COMMIT
```

**Main Node Log:**
```
(nothing - Main was down)
```

**Recovery:**
1. Main comes back online
2. Query Node A/B transaction_logs for entries with `source_node: NODE_A/B` that don't exist in Main
3. Replay those transactions on Main node
4. Update federated tables to sync

### Case 3: Node 2/3 Fails During Write from Main
**Scenario:** Main sends federated update to Node A, but Node A crashes before write completes

**Main Node Log:**
```
BEGIN → MODIFY → COMMIT (thought it succeeded)
```

**Node A Log:**
```
(nothing or incomplete transaction)
```

**Recovery:**
1. Node A comes back online
2. Compare Main's transaction_log with Node A's transaction_log
3. Find transactions in Main (source: MAIN) that don't exist in Node A
4. Replay those federated operations on Node A

### Case 4: Node 2/3 Recovers, Missed Transactions
**Scenario:** Node A was down, Main successfully wrote to Node B

**Main Node Log:**
```
BEGIN → MODIFY → COMMIT
```

**Node B Log:**
```
MODIFY (source: MAIN)
```

**Node A Log:**
```
(nothing - was offline)
```

**Recovery:**
1. Node A comes back online
2. Query Main's transaction_log for recent transactions
3. Filter for transactions that should have replicated to Node A (startYear >= 2025)
4. Check if those transactions exist in Node A's log
5. If missing, replay them

## Implementation Details

### Federated Operation Flag
Main node sets `@federated_operation = 1` before updating federated tables. This tells Node A/B triggers:
- Don't create new transaction_id (reuse Main's)
- Don't log BEGIN/COMMIT
- Do log MODIFY with `source_node: MAIN`

### Trigger Logic (Node A/B)
```sql
IF @federated_operation = 1 THEN
    -- Replicated from Main
    SET @is_local_transaction = 0;
    -- Log MODIFY only, source_node: MAIN
ELSE
    -- Direct local operation
    SET @is_local_transaction = 1;
    -- Log full BEGIN → MODIFY → COMMIT, source_node: NODE_A/B
END IF
```

## Recovery Procedures

### Stored Procedure: recover_from_main()
```sql
-- Run on Node A/B to catch up from Main
SELECT * FROM transaction_log_main 
WHERE source_node = 'MAIN'
  AND transaction_id NOT IN (SELECT transaction_id FROM transaction_log)
  AND operation_type IN ('INSERT', 'UPDATE')
ORDER BY timestamp;
-- Replay missing transactions
```

### Stored Procedure: recover_to_main()
```sql
-- Run on Main to catch up from Node A/B
SELECT * FROM transaction_log_node_a
WHERE source_node = 'NODE_A'
  AND transaction_id NOT IN (SELECT transaction_id FROM transaction_log)
ORDER BY timestamp;
-- Replay missing transactions
```

## Testing Recovery

### Test 1: Simulate Main Crash
1. Start transaction on Main
2. Kill MySQL connection before COMMIT
3. Check transaction_log for incomplete transaction
4. Verify Node A/B has replicated entry
5. Run recovery procedure

### Test 2: Simulate Node A/B Crash
1. Stop Node A database
2. Perform operations on Main
3. Restart Node A
4. Run recovery to sync missing transactions

### Test 3: Split Brain
1. Disconnect Main from Node A/B
2. Perform operations on both sides
3. Reconnect
4. Resolve conflicts using timestamp or manual review

## Monitoring Queries

### Check Incomplete Transactions
```sql
SELECT t1.transaction_id, t1.timestamp
FROM transaction_log t1
WHERE t1.log_type = 'BEGIN'
  AND NOT EXISTS (
    SELECT 1 FROM transaction_log t2 
    WHERE t2.transaction_id = t1.transaction_id 
      AND t2.log_type IN ('COMMIT', 'ABORT')
  );
```

### Check Replication Lag
```sql
SELECT 
  TIMESTAMPDIFF(SECOND, MAX(node_a.timestamp), MAX(main.timestamp)) AS lag_seconds
FROM transaction_log main
LEFT JOIN transaction_log_node_a node_a 
  ON main.transaction_id = node_a.transaction_id;
```

### Find Missing Replications
```sql
SELECT main.*
FROM transaction_log main
WHERE main.source_node = 'MAIN'
  AND main.log_type = 'MODIFY'
  AND NOT EXISTS (
    SELECT 1 FROM transaction_log_node_a
    WHERE transaction_id = main.transaction_id
  );
```

## Benefits of This Approach

1. ✅ **Independent Node Recovery** - Each node can recover independently
2. ✅ **Transaction Traceability** - Same transaction_id across all nodes
3. ✅ **Distinguishes Source** - Can tell if change came from Main or local
4. ✅ **Prevents Cascade Logging** - No duplicate log entries
5. ✅ **Enables Point-in-Time Recovery** - Timestamp-based replay
6. ✅ **Supports Conflict Detection** - Can identify concurrent modifications

## Next Steps

1. Implement recovery stored procedures
2. Create monitoring dashboard
3. Set up automated recovery checks
4. Test all 4 failure scenarios
5. Document operational runbooks
