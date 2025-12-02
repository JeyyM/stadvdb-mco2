-- ============================================================================
-- FIX FEDERATED TABLES FOR RENDER DEPLOYMENT
-- Replace internal Proxmox IPs with external Render database URLs
-- ============================================================================

USE `stadvdb-mco2`;

-- ============================================================================
-- DROP OLD FEDERATED TABLES (with wrong connection strings)
-- ============================================================================

DROP TABLE IF EXISTS title_ft_node_a;
DROP TABLE IF EXISTS title_ft_node_b;
DROP TABLE IF EXISTS transaction_log_node_a;
DROP TABLE IF EXISTS transaction_log_node_b;

-- ============================================================================
-- RECREATE WITH CORRECT RENDER URLs
-- Based on db-failover.js configuration
-- ============================================================================

-- Title_ft federated access to Node A
CREATE TABLE title_ft_node_a (
    tconst VARCHAR(12) PRIMARY KEY,
    primaryTitle VARCHAR(1024),
    runtimeMinutes SMALLINT UNSIGNED,
    averageRating DECIMAL(3,1),
    numVotes INT UNSIGNED,
    weightedRating DECIMAL(10,2),
    startYear SMALLINT UNSIGNED
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@stadvdb-mco2-a.h.filess.io:3307/stadvdb-mco2-a/title_ft';

-- Title_ft federated access to Node B
CREATE TABLE title_ft_node_b (
    tconst VARCHAR(12) PRIMARY KEY,
    primaryTitle VARCHAR(1024),
    runtimeMinutes SMALLINT UNSIGNED,
    averageRating DECIMAL(3,1),
    numVotes INT UNSIGNED,
    weightedRating DECIMAL(10,2),
    startYear SMALLINT UNSIGNED
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@stadvdb-mco2-b.h.filess.io:3307/stadvdb-mco2-b/title_ft';

-- Transaction_log federated access to Node A
CREATE TABLE transaction_log_node_a (
    log_id BIGINT,
    transaction_id VARCHAR(36),
    log_sequence INT,
    log_type ENUM('BEGIN', 'MODIFY', 'COMMIT', 'ABORT'),
    timestamp TIMESTAMP(6),
    table_name VARCHAR(64),
    record_id VARCHAR(12),
    column_name VARCHAR(64),
    old_value TEXT,
    new_value TEXT,
    operation_type ENUM('INSERT', 'UPDATE', 'DELETE'),
    source_node ENUM('MAIN', 'NODE_A', 'NODE_B')
) ENGINE=FEDERATED 
CONNECTION='mysql://g18:fuckingpassword@stadvdb-mco2-a.h.filess.io:3307/stadvdb-mco2-a/transaction_log';

-- Transaction_log federated access to Node B
CREATE TABLE transaction_log_node_b (
    log_id BIGINT,
    transaction_id VARCHAR(36),
    log_sequence INT,
    log_type ENUM('BEGIN', 'MODIFY', 'COMMIT', 'ABORT'),
    timestamp TIMESTAMP(6),
    table_name VARCHAR(64),
    record_id VARCHAR(12),
    column_name VARCHAR(64),
    old_value TEXT,
    new_value TEXT,
    operation_type ENUM('INSERT', 'UPDATE', 'DELETE'),
    source_node ENUM('MAIN', 'NODE_A', 'NODE_B')
) ENGINE=FEDERATED 
CONNECTION='mysql://g18:fuckingpassword@stadvdb-mco2-b.h.filess.io:3307/stadvdb-mco2-b/transaction_log';

-- ============================================================================
-- VERIFY SETUP
-- ============================================================================

SELECT 'Federated tables recreated with Render URLs' AS status;

-- Check all federated tables
SELECT 
    TABLE_NAME,
    ENGINE
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'stadvdb-mco2'
  AND ENGINE = 'FEDERATED';

-- ============================================================================
-- TEST CONNECTIVITY
-- ============================================================================

-- Set timeout to avoid hanging
SET SESSION MAX_EXECUTION_TIME = 10000; -- 10 seconds

SELECT 'Testing Node A title_ft...' AS test;
SELECT COUNT(*) as node_a_titles FROM title_ft_node_a;

SELECT 'Testing Node B title_ft...' AS test;
SELECT COUNT(*) as node_b_titles FROM title_ft_node_b;

SELECT 'Testing Node A transaction_log...' AS test;
SELECT COUNT(*) as node_a_logs FROM transaction_log_node_a;

SELECT 'Testing Node B transaction_log...' AS test;
SELECT COUNT(*) as node_b_logs FROM transaction_log_node_b;

-- Reset timeout
SET SESSION MAX_EXECUTION_TIME = 0;

SELECT 'All federated tables tested successfully!' AS status;
