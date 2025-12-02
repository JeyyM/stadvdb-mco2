-- ============================================================================
-- NODE A CONFIGURATION & MODE TRACKING
-- Manages Node A's role as VICE node with automatic failover capability
-- ============================================================================

USE `stadvdb-mco2-a`;

-- Create configuration table to track node mode
CREATE TABLE IF NOT EXISTS node_config (
    config_key VARCHAR(50) PRIMARY KEY,
    config_value VARCHAR(100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Set initial mode to VICE (backup/standby)
INSERT INTO node_config (config_key, config_value) 
VALUES ('node_mode', 'VICE') 
ON DUPLICATE KEY UPDATE config_value = 'VICE';

-- Track last successful connection to Main
INSERT INTO node_config (config_key, config_value) 
VALUES ('last_main_contact', NOW()) 
ON DUPLICATE KEY UPDATE config_value = NOW();

-- ============================================================================
-- FEDERATED TABLES FOR BIDIRECTIONAL SYNC
-- Node A needs access to Main and Node B for acting master role
-- ============================================================================

-- Drop existing federated tables if they exist
DROP TABLE IF EXISTS title_ft_main;
DROP TABLE IF EXISTS title_ft_node_b;
DROP TABLE IF EXISTS transaction_log_main;

-- Federated connection to Main's title_ft
CREATE TABLE title_ft_main (
    tconst VARCHAR(12),
    primaryTitle VARCHAR(1024),
    runtimeMinutes SMALLINT UNSIGNED,
    averageRating DECIMAL(3,1),
    numVotes INT UNSIGNED,
    weightedRating DECIMAL(4,2),
    startYear SMALLINT UNSIGNED,
    PRIMARY KEY (tconst)
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.51:3306/stadvdb-mco2/title_ft';

-- Federated connection to Node B's title_ft
CREATE TABLE title_ft_node_b (
    tconst VARCHAR(12),
    primaryTitle VARCHAR(1024),
    runtimeMinutes SMALLINT UNSIGNED,
    averageRating DECIMAL(3,1),
    numVotes INT UNSIGNED,
    weightedRating DECIMAL(4,2),
    startYear SMALLINT UNSIGNED,
    PRIMARY KEY (tconst)
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.53:3306/stadvdb-mco2-b/title_ft';

-- Federated connection to Main's transaction_log (for recovery)
CREATE TABLE transaction_log_main (
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
CONNECTION='mysql://g18:fuckingpassword@10.2.14.51:3306/stadvdb-mco2/transaction_log';

SELECT 'Node A configuration and federated tables created successfully' AS status;
