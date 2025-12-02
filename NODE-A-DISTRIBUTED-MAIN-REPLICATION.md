# Node A Distributed Procedures - Main Replication Support

## Overview
Updated `4-a-distributed-procedures.sql` to add Main replication capabilities when Node A acts as the acting master and Main is offline. This ensures operations are logged for recovery when Main comes back online.

## Changes Applied

### Key Improvement: Two-Phase Commit Pattern

All distributed procedures now follow:
1. **Phase 1**: Commit operation to local Node A/B (guaranteed to Node A)
2. **Phase 2**: Attempt replication to Main (best effort - gracefully handles failure)
3. **Logging**: Each operation logged to transaction_log for recovery

### 1. **distributed_insert - Updated**

**Before:** Only inserted to Node A/B, no Main replication
```sql
INSERT INTO title_ft VALUES (...);  -- or title_ft_node_b
```

**After:** Two-phase with Main replication
```sql
-- PHASE 1: Insert to Node A/B (guaranteed)
START TRANSACTION;
INSERT INTO title_ft VALUES (...);
INSERT INTO transaction_log (...) VALUES (...);
COMMIT;

-- PHASE 2: Attempt Main replication (best effort)
START TRANSACTION;
BEGIN
    DECLARE EXIT HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
    BEGIN
        SET federated_error = 1;
        ROLLBACK;
    END;
    INSERT INTO title_ft_main VALUES (...);
    COMMIT;
END;
```

### 2. **distributed_delete - Updated**

**Before:** Only deleted from Node A/B, no Main replication
```sql
DELETE FROM title_ft WHERE tconst = new_tconst;
```

**After:** Two-phase with Main replication and logging
```sql
-- PHASE 1: Delete from Node A/B (guaranteed)
START TRANSACTION;
SELECT JSON_OBJECT(...) INTO old_value_json;
DELETE FROM title_ft WHERE tconst = new_tconst;
INSERT INTO transaction_log (...) VALUES (...);
COMMIT;

-- PHASE 2: Attempt Main replication (best effort)
START TRANSACTION;
BEGIN
    DECLARE EXIT HANDLER FOR ...
    BEGIN
        SET federated_error = 1;
        ROLLBACK;
    END;
    DELETE FROM title_ft_main WHERE tconst = new_tconst;
    COMMIT;
END;
```

### 3. **distributed_addReviews - Updated**

**Before:** Only updated Node A/B, no Main replication
```sql
UPDATE title_ft SET numVotes = ..., averageRating = ..., weightedRating = ...
WHERE tconst = new_tconst;
```

**After:** Two-phase with Main replication and logging
```sql
-- PHASE 1: Update Node A/B (guaranteed)
START TRANSACTION;
UPDATE title_ft SET numVotes = ..., averageRating = ..., weightedRating = ...;
INSERT INTO transaction_log (...) VALUES (...);
COMMIT;

-- PHASE 2: Attempt Main replication (best effort)
START TRANSACTION;
BEGIN
    DECLARE EXIT HANDLER FOR ...
    BEGIN
        SET federated_error = 1;
        ROLLBACK;
    END;
    UPDATE title_ft_main SET numVotes = ..., averageRating = ..., weightedRating = ...;
    COMMIT;
END;
```

## How It Works When Main is Down

### Scenario: Node A is Acting Master, Main is Offline

1. **Insert new movie on Node A**:
   ```sql
   CALL distributed_insert('tt0000001', 'Test Movie', 150, 8.0, 5000, 2025);
   ```
   - ✅ Data committed to Node A immediately
   - ✅ Operation logged in transaction_log
   - ⚠️ Main replication attempt fails (Main is down) - flagged but doesn't affect Node A
   - Result: `⚠️ Inserted to Node A/B but Main is unreachable (will recover later): tt0000001`

2. **Update reviews on Node A**:
   ```sql
   CALL distributed_addReviews('tt0000001', 100, 8.5);
   ```
   - ✅ Data committed to Node A immediately
   - ✅ Operation logged in transaction_log
   - ⚠️ Main replication fails (Main still down) - flagged but doesn't affect Node A
   - Result: `⚠️ Updated in Node A/B but Main is unreachable (will recover later): tt0000001`

3. **Delete movie on Node A**:
   ```sql
   CALL distributed_delete('tt0000001');
   ```
   - ✅ Data deleted from Node A immediately
   - ✅ Operation logged in transaction_log
   - ⚠️ Main replication fails (Main still down)
   - Result: `⚠️ Deleted from Node A/B but Main is unreachable (will recover later): tt0000001`

### When Main Comes Back Online

1. **Run recovery from Node A** (using transaction_log that was created during Phase 1):
   ```sql
   SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = 'main';
   CALL full_recovery_main(@checkpoint);
   ```

2. **Recovery process**:
   - Reads transaction_log entries (created during all Phase 1 commits)
   - Uses LEFT JOIN to handle multi-row transactions
   - Replays each INSERT/UPDATE/DELETE to Main
   - Main is now fully synchronized

## Key Features

| Feature | Benefit |
|---------|---------|
| **Two-Phase Commit** | Node A operations never fail, even if Main is down |
| **Transaction Logging** | All Phase 1 operations logged for recovery |
| **Graceful Degradation** | Main replication failures don't cascade to Node A |
| **Error Handling** | EXIT HANDLER catches federated errors: 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430 |
| **Recovery Ready** | Main has all transaction data from transaction_log when it comes back online |

## Required: Federated Table on Node A

Node A must have a federated table pointing to Main:

```sql
CREATE TABLE IF NOT EXISTS title_ft_main (
    tconst VARCHAR(12) PRIMARY KEY,
    primaryTitle VARCHAR(1024),
    runtimeMinutes SMALLINT UNSIGNED,
    averageRating DECIMAL(3,1),
    numVotes INT UNSIGNED,
    weightedRating DECIMAL(4,2),
    startYear SMALLINT UNSIGNED
)
ENGINE=FEDERATED
CONNECTION='mysql://root:password@10.2.14.51:3306/stadvdb-mco2/title_ft';
```

## Deployment

1. Deploy updated `4-a-distributed-procedures.sql` to Node A
2. Verify federated table `title_ft_main` exists on Node A
3. Verify transaction_log table exists on Node A for logging
4. Test with Main offline to verify operations succeed on Node A
5. Bring Main back online and run recovery

## Testing Checklist

- [ ] Insert new movie on Node A with Main down
  - Verify data appears in Node A
  - Verify entry in transaction_log
  - Verify warning message about Main
  
- [ ] Update reviews on Node A with Main down
  - Verify updates in Node A
  - Verify entry in transaction_log
  
- [ ] Delete movie on Node A with Main down
  - Verify deletion in Node A
  - Verify entry in transaction_log
  
- [ ] Bring Main back online
  - Run: `SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = 'main'; CALL full_recovery_main(@checkpoint);`
  - Verify all Node A operations replayed to Main
  - Verify Main matches Node A data

## Notes

- Each procedure still handles node routing (startYear >= 2025 → Node A, else → Node B)
- Main replication is always attempted but failures are non-blocking
- Transaction logging happens in Phase 1 (guaranteed to succeed)
- Recovery uses `full_recovery_main` procedure from `5-recovery-for-main.sql`
