-- ============================================================================
-- COMPLETE RECOVERY DEPLOYMENT & EXECUTION
-- Run these commands in order on each node
-- ============================================================================

-- ============================================================================
-- STEP 1: ON MAIN - Initialize checkpoints and deploy Node A/B recovery
-- ============================================================================
-- Run on: Main (stadvdb-mco2)

USE stadvdb-mco2;

-- Initialize recovery checkpoint table and checkpoints
INSERT INTO recovery_checkpoint (node_name, last_recovery_timestamp, recovery_count)
VALUES 
  ('node_a', '2000-01-01 00:00:00.000000', 0),
  ('node_b', '2000-01-01 00:00:00.000000', 0)
ON DUPLICATE KEY UPDATE node_name = node_name;

-- Deploy recovery procedures for Node A and B
source c:\Users\asus\Desktop\Recovery\Modifications\5-main-recovery-a.sql;
source c:\Users\asus\Desktop\Recovery\Modifications\5-main-recovery-b.sql;

-- Verify procedures created
SHOW PROCEDURE STATUS WHERE Db = 'stadvdb-mco2' AND Name LIKE 'full_recovery%';

-- ============================================================================
-- STEP 2: ON NODE A - Initialize checkpoint and deploy Main recovery
-- ============================================================================
-- Run on: Node A (stadvdb-mco2-a)

USE `stadvdb-mco2-a`;

-- Initialize recovery checkpoint table and checkpoints
INSERT INTO recovery_checkpoint (node_name, last_recovery_timestamp, recovery_count)
VALUES 
  ('main', '2000-01-01 00:00:00.000000', 0)
ON DUPLICATE KEY UPDATE node_name = node_name;

-- Deploy recovery procedures for Main
source c:\Users\asus\Desktop\Recovery\Modifications\5-recovery-for-main.sql;

-- Verify procedures created
SHOW PROCEDURE STATUS WHERE Db = 'stadvdb-mco2-a' AND Name LIKE 'full_recovery%';

-- ============================================================================
-- STEP 3: ON NODE B - Initialize checkpoint (optional, if using Node B as backup master)
-- ============================================================================
-- Run on: Node B (stadvdb-mco2-b) [OPTIONAL]

USE `stadvdb-mco2-b`;

-- Initialize recovery checkpoint table and checkpoints
INSERT INTO recovery_checkpoint (node_name, last_recovery_timestamp, recovery_count)
VALUES 
  ('main', '2000-01-01 00:00:00.000000', 0)
ON DUPLICATE KEY UPDATE node_name = node_name;

-- Deploy recovery procedures for Main (same as Node A)
source c:\Users\asus\Desktop\Recovery\Modifications\5-recovery-for-main.sql;

-- ============================================================================
-- STEP 4: TEST RECOVERY - Run after setup is complete
-- ============================================================================

-- ============================================================================
-- TEST 4A: Recover Node A from Main (run on MAIN)
-- ============================================================================
-- When: After Node A was down and came back online
-- Run on: Main (stadvdb-mco2)

USE stadvdb-mco2;

SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = 'node_a';
CALL full_recovery_node_a(@checkpoint);

-- Verify checkpoint was updated
SELECT * FROM recovery_checkpoint WHERE node_name = 'node_a';

-- ============================================================================
-- TEST 4B: Recover Node B from Main (run on MAIN)
-- ============================================================================
-- When: After Node B was down and came back online
-- Run on: Main (stadvdb-mco2)

USE stadvdb-mco2;

SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = 'node_b';
CALL full_recovery_node_b(@checkpoint);

-- Verify checkpoint was updated
SELECT * FROM recovery_checkpoint WHERE node_name = 'node_b';

-- ============================================================================
-- TEST 4C: Recover Main from Node A (run on NODE A)
-- ============================================================================
-- When: After Main was down and came back online
-- Run on: Node A (stadvdb-mco2-a)

USE `stadvdb-mco2-a`;

SELECT @checkpoint := last_recovery_timestamp FROM recovery_checkpoint WHERE node_name = 'main';
CALL full_recovery_main(@checkpoint);

-- Verify checkpoint was updated
SELECT * FROM recovery_checkpoint WHERE node_name = 'main';

-- ============================================================================
-- VERIFICATION - Check all checkpoints are synchronized
-- ============================================================================

-- On Main:
USE stadvdb-mco2;
SELECT * FROM recovery_checkpoint;

-- On Node A:
USE `stadvdb-mco2-a`;
SELECT * FROM recovery_checkpoint;

-- On Node B:
USE `stadvdb-mco2-b`;
SELECT * FROM recovery_checkpoint;
