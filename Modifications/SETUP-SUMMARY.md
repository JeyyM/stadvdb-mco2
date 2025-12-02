# Failover System - Quick Setup Guide

## Files Created

1. **5-a-config.sql** - Node A configuration and federated tables setup
2. **5-main-recovery-setup.sql** - Main node federated access to Node A logs
3. **6-a-modifiers.sql** - Node A distributed procedures with failover support
4. **7-a-failover.sql** - Health check and automatic promotion/demotion
5. **8-main-recovery.sql** - Main node recovery procedures
6. **FAILOVER-README.md** - Complete documentation

## Quick Setup (Run in Order)

### Step 1: Setup Node A (Vice-Master)
```bash
# Connect to Node A and run:
mysql -h stadvdb-mco2-a.h.filess.io -P 3307 -u g18 -pfuckingpassword < 5-a-config.sql
mysql -h stadvdb-mco2-a.h.filess.io -P 3307 -u g18 -pfuckingpassword < 6-a-modifiers.sql
mysql -h stadvdb-mco2-a.h.filess.io -P 3307 -u g18 -pfuckingpassword < 7-a-failover.sql
```

### Step 2: Setup Main (Recovery Capability)
```bash
# Connect to Main and run:
mysql -h stadvdb-mco2.h.filess.io -P 3307 -u g18 -pfuckingpassword < 5-main-recovery-setup.sql
mysql -h stadvdb-mco2.h.filess.io -P 3307 -u g18 -pfuckingpassword < 8-main-recovery.sql
```

### Step 3: Verify Setup
```sql
-- On Node A:
CALL `stadvdb-mco2-a`.check_main_health();
-- Should return: "Main is healthy. Node A remains in VICE mode."

-- On Main:
SELECT COUNT(*) FROM transaction_log_node_a;
-- Should return a number (federated connection working)
```

## Testing Failover

### Test 1: Simulate Main Failure
```sql
-- On Node A, manually promote:
CALL `stadvdb-mco2-a`.promote_to_acting_master();

-- Verify mode changed:
SELECT config_value FROM node_config WHERE config_key = 'node_mode';
-- Should return: ACTING_MASTER

-- Now you can write to Node A:
CALL `stadvdb-mco2-a`.distributed_insert('ttTEST001', 'Failover Test', 90, 7.0, 1000, 2025);
```

### Test 2: Main Recovery
```sql
-- On Main, recover missed transactions:
CALL `stadvdb-mco2`.recover_from_node_a();
-- Should show: "Recovery complete. Replayed X transactions from Node A."

-- Verify record is in Main:
SELECT * FROM title_ft WHERE tconst = 'ttTEST001';

-- On Node A, demote back to VICE:
CALL `stadvdb-mco2-a`.demote_to_vice();
```

## What This Gives You

✅ **Case #1 (Main down, can't write to Main)**: Node A takes over automatically  
✅ **Case #2 (Main recovers, missed transactions)**: Recovery procedure replays from Node A logs  
✅ **Case #3 (Main tries to write when down)**: Node A handles writes independently  
✅ **Case #4 (Node A/B recovers, missed transactions)**: Already handled by federated tables  

## Next Steps

1. **Run the setup scripts** on Node A and Main
2. **Test the failover** manually to understand the process
3. **Integrate health check** into your application monitoring
4. **Update application** to retry connections on Node A if Main fails
5. **Read FAILOVER-README.md** for complete documentation

Your database now has **automatic failover** with Node A as Vice-Master! 🎉
