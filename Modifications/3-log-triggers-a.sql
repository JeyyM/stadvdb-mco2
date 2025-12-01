-- ============================================================================
-- TRANSACTION LOGGING TRIGGERS - NODE A
-- Automatically logs all INSERT, UPDATE, DELETE operations on title_ft
-- Prevents cascade logging from federated operations
-- ============================================================================

USE `stadvdb-mco2-a`;

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
    -- Only log if this is a direct operation (not from Main's federated call)
    -- Check if we're already in a transaction from Main by seeing if log already has entries
    IF @current_transaction_id IS NULL OR @federated_operation IS NULL THEN
        -- This is a direct local operation, create new transaction
        SET @current_transaction_id = UUID();
        SET @current_log_sequence = 0;
        SET @is_local_transaction = 1;
        
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, source_node, timestamp)
        VALUES (@current_transaction_id, 1, 'BEGIN', 'NODE_A', NOW(6));
        
        SET @current_log_sequence = 1;
    END IF;
END$$

CREATE TRIGGER title_ft_after_insert
AFTER INSERT ON title_ft
FOR EACH ROW
BEGIN
    -- Only log details if this is a local transaction
    IF @is_local_transaction = 1 THEN
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
         'INSERT', 'NODE_A', NOW(6));
    END IF;
END$$

-- ============================================================================
-- UPDATE TRIGGERS
-- ============================================================================

CREATE TRIGGER title_ft_before_update
BEFORE UPDATE ON title_ft
FOR EACH ROW
BEGIN
    -- Only log if this is a direct operation (not from Main's federated call)
    IF @current_transaction_id IS NULL OR @federated_operation IS NULL THEN
        -- This is a direct local operation
        SET @current_transaction_id = UUID();
        SET @current_log_sequence = 0;
        SET @is_local_transaction = 1;
        
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, source_node, timestamp)
        VALUES (@current_transaction_id, 1, 'BEGIN', 'NODE_A', NOW(6));
        
        SET @current_log_sequence = 1;
    END IF;
END$$

CREATE TRIGGER title_ft_after_update
AFTER UPDATE ON title_ft
FOR EACH ROW
BEGIN
    -- Only log details if this is a local transaction
    IF @is_local_transaction = 1 THEN
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
         'UPDATE', 'NODE_A', NOW(6));
    END IF;
END$$

-- ============================================================================
-- DELETE TRIGGERS
-- ============================================================================

CREATE TRIGGER title_ft_before_delete
BEFORE DELETE ON title_ft
FOR EACH ROW
BEGIN
    -- Only log if this is a direct operation (not from Main's federated call)
    IF @current_transaction_id IS NULL OR @federated_operation IS NULL THEN
        -- This is a direct local operation
        SET @current_transaction_id = UUID();
        SET @current_log_sequence = 0;
        SET @is_local_transaction = 1;
        
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, source_node, timestamp)
        VALUES (@current_transaction_id, 1, 'BEGIN', 'NODE_A', NOW(6));
        
        SET @current_log_sequence = 1;
    END IF;
END$$

CREATE TRIGGER title_ft_after_delete
AFTER DELETE ON title_ft
FOR EACH ROW
BEGIN
    -- Only log details if this is a local transaction
    IF @is_local_transaction = 1 THEN
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
         'DELETE', 'NODE_A', NOW(6));
    END IF;
END$$

DELIMITER ;

SELECT 'Node A triggers created successfully' AS status;
