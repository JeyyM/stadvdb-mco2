-- ============================================================================
-- TRANSACTION LOGGING TRIGGERS - NODE B
-- Automatically logs all INSERT, UPDATE, DELETE operations on title_ft
-- Implements Write-Ahead Logging with deferred modification
-- ============================================================================

USE `stadvdb-mco2-b`;

-- Drop existing triggers if they exist
DROP TRIGGER IF EXISTS title_ft_before_insert;
DROP TRIGGER IF EXISTS title_ft_after_insert;
DROP TRIGGER IF EXISTS title_ft_before_update;
DROP TRIGGER IF EXISTS title_ft_after_update;
DROP TRIGGER IF EXISTS title_ft_before_delete;
DROP TRIGGER IF EXISTS title_ft_after_delete;

DELIMITER $$

-- ============================================================================
-- INSERT TRIGGERS
-- ============================================================================

CREATE TRIGGER title_ft_before_insert
BEFORE INSERT ON title_ft
FOR EACH ROW
BEGIN
    DECLARE current_transaction_id VARCHAR(36);
    DECLARE current_sequence INT;
    
    SET current_transaction_id = IFNULL(@current_transaction_id, UUID());
    SET @current_transaction_id = current_transaction_id;
    SET current_sequence = IFNULL(@current_log_sequence, 0);
    
    IF current_sequence = 0 THEN
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, source_node, timestamp)
        VALUES (current_transaction_id, 1, 'BEGIN', 'NODE_B', NOW(6));
        
        SET @current_log_sequence = 1;
    END IF;
END$$

CREATE TRIGGER title_ft_after_insert
AFTER INSERT ON title_ft
FOR EACH ROW
BEGIN
    SET @current_log_sequence = @current_log_sequence + 1;
    
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, table_name, record_id, 
     column_name, old_value, new_value, operation_type, source_node, timestamp)
    VALUES 
    (@current_transaction_id, @current_log_sequence, 'MODIFY', 'title_ft', NEW.tconst,
     'ALL_COLUMNS', 
     NULL,
     JSON_OBJECT(
         'tconst', NEW.tconst,
         'primaryTitle', NEW.primaryTitle,
         'runtimeMinutes', NEW.runtimeMinutes,
         'averageRating', NEW.averageRating,
         'numVotes', NEW.numVotes,
         'weightedRating', NEW.weightedRating,
         'startYear', NEW.startYear
     ),
     'INSERT', 'NODE_B', NOW(6));
END$$

-- ============================================================================
-- UPDATE TRIGGERS
-- ============================================================================

CREATE TRIGGER title_ft_before_update
BEFORE UPDATE ON title_ft
FOR EACH ROW
BEGIN
    DECLARE current_transaction_id VARCHAR(36);
    DECLARE current_sequence INT;
    
    SET current_transaction_id = IFNULL(@current_transaction_id, UUID());
    SET @current_transaction_id = current_transaction_id;
    SET current_sequence = IFNULL(@current_log_sequence, 0);
    
    IF current_sequence = 0 THEN
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, source_node, timestamp)
        VALUES (current_transaction_id, 1, 'BEGIN', 'NODE_B', NOW(6));
        
        SET @current_log_sequence = 1;
    END IF;
END$$

CREATE TRIGGER title_ft_after_update
AFTER UPDATE ON title_ft
FOR EACH ROW
BEGIN
    SET @current_log_sequence = @current_log_sequence + 1;
    
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, table_name, record_id, 
     column_name, old_value, new_value, operation_type, source_node, timestamp)
    VALUES 
    (@current_transaction_id, @current_log_sequence, 'MODIFY', 'title_ft', NEW.tconst,
     'ALL_COLUMNS',
     JSON_OBJECT(
         'tconst', OLD.tconst,
         'primaryTitle', OLD.primaryTitle,
         'runtimeMinutes', OLD.runtimeMinutes,
         'averageRating', OLD.averageRating,
         'numVotes', OLD.numVotes,
         'weightedRating', OLD.weightedRating,
         'startYear', OLD.startYear
     ),
     JSON_OBJECT(
         'tconst', NEW.tconst,
         'primaryTitle', NEW.primaryTitle,
         'runtimeMinutes', NEW.runtimeMinutes,
         'averageRating', NEW.averageRating,
         'numVotes', NEW.numVotes,
         'weightedRating', NEW.weightedRating,
         'startYear', NEW.startYear
     ),
     'UPDATE', 'NODE_B', NOW(6));
END$$

-- ============================================================================
-- DELETE TRIGGERS
-- ============================================================================

CREATE TRIGGER title_ft_before_delete
BEFORE DELETE ON title_ft
FOR EACH ROW
BEGIN
    DECLARE current_transaction_id VARCHAR(36);
    DECLARE current_sequence INT;
    
    SET current_transaction_id = IFNULL(@current_transaction_id, UUID());
    SET @current_transaction_id = current_transaction_id;
    SET current_sequence = IFNULL(@current_log_sequence, 0);
    
    IF current_sequence = 0 THEN
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, source_node, timestamp)
        VALUES (current_transaction_id, 1, 'BEGIN', 'NODE_B', NOW(6));
        
        SET @current_log_sequence = 1;
    END IF;
END$$

CREATE TRIGGER title_ft_after_delete
AFTER DELETE ON title_ft
FOR EACH ROW
BEGIN
    SET @current_log_sequence = @current_log_sequence + 1;
    
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, table_name, record_id, 
     column_name, old_value, new_value, operation_type, source_node, timestamp)
    VALUES 
    (@current_transaction_id, @current_log_sequence, 'MODIFY', 'title_ft', OLD.tconst,
     'ALL_COLUMNS',
     JSON_OBJECT(
         'tconst', OLD.tconst,
         'primaryTitle', OLD.primaryTitle,
         'runtimeMinutes', OLD.runtimeMinutes,
         'averageRating', OLD.averageRating,
         'numVotes', OLD.numVotes,
         'weightedRating', OLD.weightedRating,
         'startYear', OLD.startYear
     ),
     NULL,
     'DELETE', 'NODE_B', NOW(6));
END$$

DELIMITER ;

SELECT 'Node B triggers created successfully' AS status;
