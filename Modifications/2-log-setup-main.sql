-- ============================================================================
-- TRANSACTION LOGGING SETUP
-- Creates transaction_log table for Write-Ahead Logging (WAL)
-- Implements Deferred Database Modification pattern
-- ============================================================================

-- ============================================================================
-- MAIN NODE SETUP
-- ============================================================================
USE `stadvdb-mco2`;

DROP TABLE IF EXISTS transaction_log;

CREATE TABLE IF NOT EXISTS transaction_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(36) NOT NULL,
    log_sequence INT NOT NULL,
    log_type ENUM('BEGIN', 'MODIFY', 'COMMIT', 'ABORT') NOT NULL,
    timestamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    -- For MODIFY entries only
    table_name VARCHAR(64),
    record_id VARCHAR(12),
    column_name VARCHAR(64),
    old_value TEXT,
    new_value TEXT,
    
    -- Metadata
    operation_type ENUM('INSERT', 'UPDATE', 'DELETE'),
    source_node ENUM('MAIN', 'NODE_A', 'NODE_B') NOT NULL,
    
    -- Indexes for performance
    INDEX index_transaction (transaction_id, log_sequence),
    INDEX index_timestamp (timestamp),
    INDEX index_type_status (log_type, transaction_id),
    INDEX index_record (record_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Create federated tables to access other nodes' logs
-- COMMENTED OUT: These fail when Node A/B are unreachable via internal IPs
-- Uncomment and fix CONNECTION strings when network connectivity is resolved dfff

/*
DROP TABLE IF EXISTS transaction_log_node_a;
DROP TABLE IF EXISTS transaction_log_node_b;

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
CONNECTION='mysql://g18:fuckingpassword@10.2.14.52:3306/stadvdb-mco2-a/transaction_log';

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
CONNECTION='mysql://g18:fuckingpassword@10.2.14.53:3306/stadvdb-mco2-b/transaction_log';
*/

SELECT 'Main node transaction logging setup complete' AS status;
