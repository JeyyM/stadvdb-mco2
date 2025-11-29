-- ============================================================================
-- TRANSACTION LOGGING SETUP - NODE B
-- Creates transaction_log table for Write-Ahead Logging (WAL)
-- Implements Deferred Database Modification pattern
-- ============================================================================

USE `stadvdb-mco2-b`;

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
DROP TABLE IF EXISTS transaction_log_main;
DROP TABLE IF EXISTS transaction_log_node_a;

CREATE TABLE transaction_log_main (
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
CONNECTION='mysql://root:12345@ccscloud.dlsu.edu.ph:60751/stadvdb-mco2/transaction_log';

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
CONNECTION='mysql://root:12345@ccscloud.dlsu.edu.ph:60752/stadvdb-mco2-a/transaction_log';

SELECT 'Node B transaction logging setup complete' AS status;
