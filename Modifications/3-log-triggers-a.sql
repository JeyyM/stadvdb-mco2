-- TRANSACTION LOGGING TRIGGERS - NODE A
-- Automatically logs all INSERT, UPDATE, DELETE operations on title_ft
-- Prevents cascade logging from federated operations

USE `stadvdb-mco2-a`;

-- Drop existing triggers if they exist
DROP TRIGGER IF EXISTS title_ft_before_insert;
DROP TRIGGER IF EXISTS title_ft_after_insert;
DROP TRIGGER IF EXISTS title_ft_before_update;
DROP TRIGGER IF EXISTS title_ft_after_update;
DROP TRIGGER IF EXISTS title_ft_before_delete;
DROP TRIGGER IF EXISTS title_ft_after_delete;

DELIMITER $$

-- INSERT TRIGGERS

CREATE TRIGGER title_ft_before_insert
BEFORE INSERT ON title_ft
FOR EACH ROW
BEGIN
    -- Only log if this is NOT a federated operation from Main
    -- @federated_operation is explicitly set to 1 when Main calls via federated table
    IF IFNULL(@federated_operation, 0) = 0 THEN
        -- This is a direct local operation
        -- Initialize transaction if not already set by stored procedure
        IF @current_transaction_id IS NULL THEN
            SET @current_transaction_id = UUID();
        END IF;
        
        IF @current_log_sequence IS NULL THEN
            SET @current_log_sequence = 0;
            
            INSERT INTO transaction_log 
            (transaction_id, log_sequence, log_type, source_node, timestamp)
            VALUES (@current_transaction_id, 1, 'BEGIN', 'NODE_A', NOW(6));
            
            SET @current_log_sequence = 1;
        END IF;
    END IF;
END$$

CREATE TRIGGER title_ft_after_insert
AFTER INSERT ON title_ft
FOR EACH ROW
BEGIN
    -- Only log details if this is NOT a federated operation
    IF IFNULL(@federated_operation, 0) = 0 AND @current_transaction_id IS NOT NULL THEN
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
        
        -- Log COMMIT
        SET @current_log_sequence = @current_log_sequence + 1;
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, source_node, timestamp)
        VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'NODE_A', NOW(6));
        
        -- Clear session variables
        SET @current_transaction_id = NULL;
        SET @current_log_sequence = NULL;
    END IF;
END$$

-- UPDATE TRIGGERS

CREATE TRIGGER title_ft_before_update
BEFORE UPDATE ON title_ft
FOR EACH ROW
BEGIN
    -- Only log if this is NOT a federated operation from Main
    IF IFNULL(@federated_operation, 0) = 0 THEN
        IF @current_transaction_id IS NULL THEN
            SET @current_transaction_id = UUID();
        END IF;
        
        IF @current_log_sequence IS NULL THEN
            SET @current_log_sequence = 0;
            
            INSERT INTO transaction_log 
            (transaction_id, log_sequence, log_type, source_node, timestamp)
            VALUES (@current_transaction_id, 1, 'BEGIN', 'NODE_A', NOW(6));
            
            SET @current_log_sequence = 1;
        END IF;
    END IF;
END$$

CREATE TRIGGER title_ft_after_update
AFTER UPDATE ON title_ft
FOR EACH ROW
BEGIN
    -- Only log details if this is NOT a federated operation
    IF IFNULL(@federated_operation, 0) = 0 AND @current_transaction_id IS NOT NULL THEN
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
        
        -- Log COMMIT
        SET @current_log_sequence = @current_log_sequence + 1;
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, source_node, timestamp)
        VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'NODE_A', NOW(6));
        
        -- Clear session variables
        SET @current_transaction_id = NULL;
        SET @current_log_sequence = NULL;
    END IF;
END$$

-- DELETE TRIGGERS

CREATE TRIGGER title_ft_before_delete
BEFORE DELETE ON title_ft
FOR EACH ROW
BEGIN
    -- Only log if this is NOT a federated operation from Main
    IF IFNULL(@federated_operation, 0) = 0 THEN
        IF @current_transaction_id IS NULL THEN
            SET @current_transaction_id = UUID();
        END IF;
        
        IF @current_log_sequence IS NULL THEN
            SET @current_log_sequence = 0;
            
            INSERT INTO transaction_log 
            (transaction_id, log_sequence, log_type, source_node, timestamp)
            VALUES (@current_transaction_id, 1, 'BEGIN', 'NODE_A', NOW(6));
            
            SET @current_log_sequence = 1;
        END IF;
    END IF;
END$$

CREATE TRIGGER title_ft_after_delete
AFTER DELETE ON title_ft
FOR EACH ROW
BEGIN
    -- Only log details if this is NOT a federated operation
    IF IFNULL(@federated_operation, 0) = 0 AND @current_transaction_id IS NOT NULL THEN
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
        
        -- Log COMMIT
        SET @current_log_sequence = @current_log_sequence + 1;
        INSERT INTO transaction_log 
        (transaction_id, log_sequence, log_type, source_node, timestamp)
        VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'NODE_A', NOW(6));
        
        -- Clear session variables
        SET @current_transaction_id = NULL;
        SET @current_log_sequence = NULL;
    END IF;
END$$

DELIMITER ;

SELECT 'Node A triggers created successfully' AS status;
