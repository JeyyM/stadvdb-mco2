-- ============================================================================
-- TEST FEDERATED TABLE CONNECTIONS
-- Run these queries to diagnose federated table connectivity issues
-- ============================================================================

USE `stadvdb-mco2`;

-- ============================================================================
-- 1. CHECK IF FEDERATED TABLES EXIST
-- ============================================================================
SELECT 
    TABLE_NAME,
    ENGINE,
    TABLE_COMMENT
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'stadvdb-mco2'
  AND ENGINE = 'FEDERATED';

-- ============================================================================
-- 2. CHECK FEDERATED ENGINE IS ENABLED
-- ============================================================================
SHOW ENGINES;

-- Check if federated is installed
SELECT * FROM information_schema.ENGINES WHERE ENGINE = 'FEDERATED';

-- ============================================================================
-- 3. CHECK CONNECTION STRINGS (won't show password)
-- ============================================================================
SHOW CREATE TABLE transaction_log_node_a;
SHOW CREATE TABLE transaction_log_node_b;

-- ============================================================================
-- 4. TEST SIMPLE QUERY WITH TIMEOUT
-- ============================================================================
-- Set a statement timeout to prevent hanging
SET SESSION MAX_EXECUTION_TIME = 5000; -- 5 seconds

-- Try to count rows (this will timeout if connection fails)
SELECT COUNT(*) FROM transaction_log_node_a;
SELECT COUNT(*) FROM transaction_log_node_b;

-- Reset timeout
SET SESSION MAX_EXECUTION_TIME = 0;

-- ============================================================================
-- 5. CHECK CURRENT FEDERATED CONNECTIONS IN PROCESSLIST
-- ============================================================================
-- Look for connections TO Node A/B (10.2.14.52 and 10.2.14.53)
SHOW PROCESSLIST;

-- More detailed view
SELECT 
    ID,
    USER,
    HOST,
    DB,
    COMMAND,
    TIME,
    STATE,
    INFO
FROM information_schema.PROCESSLIST
WHERE HOST LIKE '10.2.14.%';

-- ============================================================================
-- 6. DROP AND RECREATE FEDERATED TABLES (if needed)
-- ============================================================================
/*
-- Drop existing federated tables
DROP TABLE IF EXISTS transaction_log_node_a;
DROP TABLE IF EXISTS transaction_log_node_b;
DROP TABLE IF EXISTS title_ft_node_a;
DROP TABLE IF EXISTS title_ft_node_b;

-- Recreate with correct connection strings
-- NOTE: Update the CONNECTION string based on your actual deployment
*/

-- ============================================================================
-- 7. TEST DIRECT CONNECTION (From MySQL command line)
-- ============================================================================
/*
From a separate MySQL session, try to connect directly to Node A:

mysql -h 10.2.14.52 -P 3306 -u g18 -pfuckingpassword stadvdb-mco2-a

If this fails, the federated tables can't work either.
*/

-- ============================================================================
-- 8. CHECK FOR FEDERATED TABLE ERRORS IN ERROR LOG
-- ============================================================================
SHOW VARIABLES LIKE 'log_error';

-- ============================================================================
-- 9. KILL STUCK FEDERATED QUERIES
-- ============================================================================
-- If a query is stuck trying to access federated table, find its ID and:
-- KILL <process_id>;

-- Find all queries accessing federated tables
SELECT 
    ID,
    USER,
    TIME,
    STATE,
    INFO
FROM information_schema.PROCESSLIST
WHERE INFO LIKE '%transaction_log_node_%'
   OR INFO LIKE '%title_ft_node_%';

-- ============================================================================
-- 10. ALTERNATIVE: CHECK IF NODES ARE ON RENDER (not internal IPs)
-- ============================================================================
-- If you're on Render, the internal IPs (10.2.14.x) won't work
-- You need to use the external database URLs instead

-- Check what your current connections look like:
SELECT 
    TABLE_NAME,
    CREATE_OPTIONS
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'stadvdb-mco2'
  AND ENGINE = 'FEDERATED';

-- ============================================================================
-- 11. QUICK FIX: DROP FEDERATED TABLES IF THEY DON'T WORK
-- ============================================================================
/*
If federated tables are causing issues and you can't fix connectivity:

DROP TABLE IF EXISTS transaction_log_node_a;
DROP TABLE IF EXISTS transaction_log_node_b;
DROP TABLE IF EXISTS title_ft_node_a;
DROP TABLE IF EXISTS title_ft_node_b;

Then use the simplified procedures that don't rely on federated tables.
*/

SELECT 'Federated table diagnostic queries ready' AS status;
