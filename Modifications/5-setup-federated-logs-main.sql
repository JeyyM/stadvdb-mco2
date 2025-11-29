-- ============================================================================
-- MAIN NODE PART 2 - Federated Transaction Log Access
-- Run this AFTER Node A and Node B are set up with their transaction_log tables
-- This enables Main to write logs to Node A/B for complete distributed logging
-- ============================================================================

USE `stadvdb-mco2`;

-- Create federated tables to access Node A/B transaction logs for remote logging
DROP TABLE IF EXISTS `transaction_log_node_a`;

CREATE TABLE `transaction_log_node_a` (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(36) NOT NULL,
    log_sequence INT NOT NULL,
    log_type ENUM('BEGIN', 'MODIFY', 'COMMIT', 'ABORT') NOT NULL,
    timestamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    table_name VARCHAR(64),
    record_id VARCHAR(12),
    column_name VARCHAR(64),
    old_value TEXT,
    new_value TEXT,
    operation_type ENUM('INSERT', 'UPDATE', 'DELETE'),
    source_node ENUM('MAIN', 'NODE_A', 'NODE_B') NOT NULL
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.52:3306/stadvdb-mco2-a/transaction_log';

DROP TABLE IF EXISTS `transaction_log_node_b`;

CREATE TABLE `transaction_log_node_b` (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(36) NOT NULL,
    log_sequence INT NOT NULL,
    log_type ENUM('BEGIN', 'MODIFY', 'COMMIT', 'ABORT') NOT NULL,
    timestamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    table_name VARCHAR(64),
    record_id VARCHAR(12),
    column_name VARCHAR(64),
    old_value TEXT,
    new_value TEXT,
    operation_type ENUM('INSERT', 'UPDATE', 'DELETE'),
    source_node ENUM('MAIN', 'NODE_A', 'NODE_B') NOT NULL
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.53:3306/stadvdb-mco2-b/transaction_log';

-- Verify setup
SELECT 'Federated transaction log tables created successfully' AS status;
SELECT COUNT(*) as node_a_logs FROM `transaction_log_node_a`;
SELECT COUNT(*) as node_b_logs FROM `transaction_log_node_b`;
