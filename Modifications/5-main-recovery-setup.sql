-- ============================================================================
-- MAIN NODE - ADD FEDERATED ACCESS TO NODE A'S TRANSACTION LOG
-- Required for Main to recover transactions from Node A after failover
-- ============================================================================

USE `stadvdb-mco2`;

-- Drop existing federated table if exists
DROP TABLE IF EXISTS transaction_log_node_a;

-- Federated connection to Node A's transaction_log
CREATE TABLE transaction_log_node_a (
    log_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(36) NOT NULL,
    log_sequence INT NOT NULL,
    log_type ENUM('BEGIN', 'MODIFY', 'COMMIT', 'ABORT') NOT NULL,
    table_name VARCHAR(64),
    record_id VARCHAR(255),
    column_name VARCHAR(64),
    old_value TEXT,
    new_value TEXT,
    operation_type ENUM('INSERT', 'UPDATE', 'DELETE'),
    source_node VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP(6) NOT NULL,
    INDEX idx_transaction (transaction_id, log_sequence),
    INDEX idx_timestamp (timestamp),
    INDEX idx_record (table_name, record_id)
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.52:3306/stadvdb-mco2-a/transaction_log';

SELECT 'Main node federated access to Node A transaction log created successfully' AS status;
