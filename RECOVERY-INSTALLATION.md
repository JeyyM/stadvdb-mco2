# Recovery System Installation Guide

## File Summary

| File | Run On | Purpose |
|------|--------|---------|
| `5-main-recovery-a.sql` | **Main** | Creates procedures for Main to recover Node A |
| `5-main-recovery-b.sql` | **Main** | Creates procedures for Main to recover Node B |
| `5-recovery-for-main.sql` | **Node A and Node B** | Creates procedures for nodes to recover Main |

## Installation Commands

### On Proxmox Main (10.2.14.51:60751)
```bash
ssh g18@ccscloud.dlsu.edu.ph -p 60751

# Install Node A recovery
mysql -u g18 -pfuckingpassword stadvdb-mco2 < 5-main-recovery-a.sql

# Install Node B recovery
mysql -u g18 -pfuckingpassword stadvdb-mco2 < 5-main-recovery-b.sql
```

### On Proxmox Node A (10.2.14.52:60752)
```bash
ssh g18@ccscloud.dlsu.edu.ph -p 60752

# Install Main recovery (from Node A's perspective)
mysql -u g18 -pfuckingpassword stadvdb-mco2-a < 5-recovery-for-main.sql
```

### On Proxmox Node B (10.2.14.53:60753)
```bash
ssh g18@ccscloud.dlsu.edu.ph -p 60753

# Install Main recovery (from Node B's perspective)
mysql -u g18 -pfuckingpassword stadvdb-mco2-b < 5-recovery-for-main.sql
```

## How It Works

### Scenario 1: Node A Goes Down
1. Node A wakes up
2. Main's backend (on Render) automatically runs: `CALL full_recovery_node_a(NOW() - INTERVAL 24 HOUR)`
3. Main reads its `transaction_log`
4. Finds all transactions for Node A partition (startYear >= 2025) after last checkpoint
5. Replays them to Node A via `title_ft_node_a` federated table
6. Saves checkpoint to prevent duplicates

### Scenario 2: Node B Goes Down
1. Node B wakes up
2. Main's backend automatically runs: `CALL full_recovery_node_b(NOW() - INTERVAL 24 HOUR)`
3. Main reads its `transaction_log`
4. Finds all transactions for Node B partition (startYear < 2025) after last checkpoint
5. Replays them to Node B via `title_ft_node_b` federated table
6. Saves checkpoint

### Scenario 3: Main Goes Down (Manual Recovery Required)
1. Main wakes up
2. **You manually run** on Node A or Node B:
   ```sql
   CALL full_recovery_main(NOW() - INTERVAL 24 HOUR);
   ```
3. Node A/B reads `transaction_log_main` (federated from Main)
4. Replays all transactions to Main via `title_ft` federated table
5. Saves checkpoint

## Checkpoint System

Each node has a `recovery_checkpoint` table:
```sql
CREATE TABLE recovery_checkpoint (
    node_name VARCHAR(50) PRIMARY KEY,          -- 'node_a', 'node_b', or 'main'
    last_recovery_timestamp TIMESTAMP(6),       -- Last successfully recovered transaction time
    recovery_count INT UNSIGNED,                -- Total transactions recovered
    last_transaction_id VARCHAR(50),            -- Last transaction ID recovered
    updated_at TIMESTAMP(6)                     -- When checkpoint was last updated
);
```

## Automatic Recovery (from backend)

Your `recovery.js` automatically calls:
- `full_recovery_node_a()` - When Main wakes up
- `full_recovery_node_b()` - When Main wakes up

**Main recovery is NOT automatic** - you need to manually trigger it if Main goes down.

## Manual Recovery Examples

```sql
-- On Main: Recover Node A for last 24 hours
CALL full_recovery_node_a(NOW() - INTERVAL 24 HOUR);

-- On Main: Recover Node B for last week
CALL full_recovery_node_b(NOW() - INTERVAL 7 DAY);

-- On Node A: Recover Main for last 24 hours
CALL full_recovery_main(NOW() - INTERVAL 24 HOUR);

-- Check checkpoint status
SELECT * FROM recovery_checkpoint;

-- Find missing transactions before running recovery
CALL find_missing_on_node_a(NOW() - INTERVAL 24 HOUR);
CALL find_missing_on_node_b(NOW() - INTERVAL 24 HOUR);
CALL find_missing_on_main(NOW() - INTERVAL 24 HOUR);
```

## Clear All Logs (Fresh Start)

```bash
# On Main:
mysql -u g18 -pfuckingpassword stadvdb-mco2 -e "TRUNCATE TABLE transaction_log; TRUNCATE TABLE recovery_checkpoint;"

# On Node A:
mysql -u g18 -pfuckingpassword stadvdb-mco2-a -e "TRUNCATE TABLE transaction_log; TRUNCATE TABLE recovery_checkpoint;"

# On Node B:
mysql -u g18 -pfuckingpassword stadvdb-mco2-b -e "TRUNCATE TABLE transaction_log; TRUNCATE TABLE recovery_checkpoint;"
```

## Victory Checklist

- [ ] Run `5-main-recovery-a.sql` on Proxmox Main
- [ ] Run `5-main-recovery-b.sql` on Proxmox Main
- [ ] Run `5-recovery-for-main.sql` on Proxmox Node A
- [ ] Run `5-recovery-for-main.sql` on Proxmox Node B
- [ ] Clear all transaction logs (optional fresh start)
- [ ] Redeploy Render backends (should auto-recover on startup)
- [ ] Test: Turn off Node A, make changes on Main, turn Node A back on
- [ ] Verify: Check `recovery_checkpoint` table shows updated timestamp
