-- ============================================================================
-- NODE A HEALTH CHECK AND AUTOMATIC FAILOVER
-- Monitors Main node health and automatically promotes Node A when Main fails
-- ============================================================================

USE `stadvdb-mco2-a`;

DROP PROCEDURE IF EXISTS check_main_health;
DROP PROCEDURE IF EXISTS promote_to_acting_master;
DROP PROCEDURE IF EXISTS demote_to_vice;

DELIMITER $$

-- ============================================================================
-- CHECK MAIN HEALTH - Should be called periodically (e.g., every 5 seconds)
-- ============================================================================

CREATE PROCEDURE check_main_health()
BEGIN
    DECLARE main_alive BOOLEAN DEFAULT FALSE;
    DECLARE current_mode VARCHAR(20);
    DECLARE last_contact TIMESTAMP;
    
    -- Try to ping Main node via federated table
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            -- Main is unreachable
            SET main_alive = FALSE;
        END;
        
        -- Attempt simple query to Main
        SELECT 1 FROM title_ft_main LIMIT 1 INTO @dummy;
        SET main_alive = TRUE;
    END;
    
    -- Get current mode
    SELECT config_value INTO current_mode 
    FROM node_config 
    WHERE config_key = 'node_mode';
    
    IF main_alive = TRUE THEN
        -- Main is online
        UPDATE node_config 
        SET config_value = NOW() 
        WHERE config_key = 'last_main_contact';
        
        -- If we're in ACTING_MASTER mode and Main is back, prepare for demotion
        IF current_mode = 'ACTING_MASTER' THEN
            SELECT 'Main is back online. Node A should demote after synchronization.' AS status;
            -- Note: Manual demotion required after Main recovers data
        ELSE
            SELECT 'Main is healthy. Node A remains in VICE mode.' AS status;
        END IF;
    ELSE
        -- Main is down
        SELECT config_value INTO last_contact
        FROM node_config 
        WHERE config_key = 'last_main_contact';
        
        IF current_mode = 'VICE' THEN
            -- Promote to ACTING_MASTER
            SELECT CONCAT('Main unreachable since ', last_contact, '. Promoting Node A to ACTING_MASTER.') AS status;
            CALL promote_to_acting_master();
        ELSE
            SELECT 'Main still down. Node A continues as ACTING_MASTER.' AS status;
        END IF;
    END IF;
END$$

-- ============================================================================
-- PROMOTE TO ACTING MASTER - Activate write capabilities
-- ============================================================================

CREATE PROCEDURE promote_to_acting_master()
BEGIN
    DECLARE current_mode VARCHAR(20);
    
    SELECT config_value INTO current_mode 
    FROM node_config 
    WHERE config_key = 'node_mode';
    
    IF current_mode = 'VICE' THEN
        -- Log promotion event
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, table_name, operation_type, source_node, timestamp)
        VALUES (UUID(), 1, 'BEGIN', 'node_config', 'UPDATE', 'NODE_A_PROMOTION', NOW(6));
        
        -- Change mode to ACTING_MASTER
        UPDATE node_config 
        SET config_value = 'ACTING_MASTER' 
        WHERE config_key = 'node_mode';
        
        -- Log commit
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, table_name, operation_type, source_node, timestamp)
        VALUES (LAST_INSERT_ID(), 2, 'COMMIT', 'node_config', 'UPDATE', 'NODE_A_PROMOTION', NOW(6));
        
        SELECT 'Node A promoted to ACTING_MASTER successfully. Node A can now accept write operations.' AS status;
    ELSE
        SELECT 'Node A is already in ACTING_MASTER mode.' AS status;
    END IF;
END$$

-- ============================================================================
-- DEMOTE TO VICE - Return to backup mode after Main recovers
-- ============================================================================

CREATE PROCEDURE demote_to_vice()
BEGIN
    DECLARE current_mode VARCHAR(20);
    
    SELECT config_value INTO current_mode 
    FROM node_config 
    WHERE config_key = 'node_mode';
    
    IF current_mode = 'ACTING_MASTER' THEN
        -- Log demotion event
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, table_name, operation_type, source_node, timestamp)
        VALUES (UUID(), 1, 'BEGIN', 'node_config', 'UPDATE', 'NODE_A_DEMOTION', NOW(6));
        
        -- Change mode back to VICE
        UPDATE node_config 
        SET config_value = 'VICE' 
        WHERE config_key = 'node_mode';
        
        -- Update last contact with Main
        UPDATE node_config 
        SET config_value = NOW() 
        WHERE config_key = 'last_main_contact';
        
        -- Log commit
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, table_name, operation_type, source_node, timestamp)
        VALUES (LAST_INSERT_ID(), 2, 'COMMIT', 'node_config', 'UPDATE', 'NODE_A_DEMOTION', NOW(6));
        
        SELECT 'Node A demoted back to VICE mode. Main node is primary again.' AS status;
    ELSE
        SELECT 'Node A is already in VICE mode.' AS status;
    END IF;
END$$

DELIMITER ;

SELECT 'Node A health check and failover procedures created successfully' AS status;
