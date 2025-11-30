-- TRANSACTION LOGGING SETUP - NODE A

USE `stadvdb-mco2-a`;

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

SELECT 'Node A transaction logging setup complete' AS status;
