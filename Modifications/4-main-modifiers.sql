USE `stadvdb-mco2`;

DROP PROCEDURE IF EXISTS distributed_insert;
DROP PROCEDURE IF EXISTS distributed_update;
DROP PROCEDURE IF EXISTS distributed_delete;
DROP PROCEDURE IF EXISTS distributed_addReviews;
DROP PROCEDURE IF EXISTS distributed_select;
DROP PROCEDURE IF EXISTS distributed_search;
DROP PROCEDURE IF EXISTS distributed_aggregation;
DROP PROCEDURE IF EXISTS log_to_remote_node;

DELIMITER $$

-- Helper procedure to log to remote node's transaction_log
-- This procedure has built-in error handling to gracefully handle node failures
CREATE PROCEDURE log_to_remote_node(
    IN target_node VARCHAR(10),  -- 'NODE_A' or 'NODE_B'
    IN txn_id VARCHAR(36),
    IN seq INT,
    IN log_t VARCHAR(10),  -- 'BEGIN', 'MODIFY', 'COMMIT', 'ABORT'
    IN tbl_name VARCHAR(64),
    IN rec_id VARCHAR(12),
    IN col_name VARCHAR(64),
    IN old_val TEXT,
    IN new_val TEXT,
    IN op_type VARCHAR(10)  -- 'INSERT', 'UPDATE', 'DELETE'
)
BEGIN
    -- Handler for federated errors when logging to remote nodes
    -- Error codes: 1429 (can't connect), 1158, 1159, 1189 (timeouts), 
    --              2013, 2006 (connection lost), 1296 (federated error wrapper), 1430 (query on foreign data source)
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
    BEGIN
        -- Silent failure - recovery system will replay these logs when node comes back
    END;
    
    IF target_node = 'NODE_A' THEN
        INSERT INTO transaction_log_node_a
        (transaction_id, log_sequence, log_type, table_name, record_id, 
         column_name, old_value, new_value, operation_type, source_node, timestamp)
        VALUES (txn_id, seq, log_t, tbl_name, rec_id, col_name, old_val, new_val, op_type, 'MAIN', NOW(6));
    ELSEIF target_node = 'NODE_B' THEN
        INSERT INTO transaction_log_node_b
        (transaction_id, log_sequence, log_type, table_name, record_id, 
         column_name, old_value, new_value, operation_type, source_node, timestamp)
        VALUES (txn_id, seq, log_t, tbl_name, rec_id, col_name, old_val, new_val, op_type, 'MAIN', NOW(6));
    END IF;
END$$

DELIMITER ;

DELIMITER $$

CREATE PROCEDURE distributed_insert(
    IN new_tconst VARCHAR(12),
    IN new_primaryTitle VARCHAR(1024),
    IN new_runtimeMinutes SMALLINT UNSIGNED,
    IN new_averageRating DECIMAL(3,1),
    IN new_numVotes INT UNSIGNED,
    IN new_startYear SMALLINT UNSIGNED
)
BEGIN
    DECLARE global_mean DECIMAL(3,1);
    DECLARE min_votes_threshold INT;
    DECLARE calculated_weightedRating DECIMAL(4,2);
    DECLARE current_transaction_id VARCHAR(36);
    DECLARE federated_error INT DEFAULT 0;
    
    -- Handler for federated table errors - set flag and continue
    -- Error codes: 1429 (can't connect), 1158 (communication error), 1159 (net timeout), 
    --              1189 (net read timeout), 2013 (lost connection), 2006 (server gone),
    --              1296 (federated error wrapper), 1430 (query on foreign data source)
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
    BEGIN
        SET federated_error = 1;
        -- Main operation succeeds, federated replication failed
        -- Recovery system will sync when nodes come back online
    END;
    
    -- Set isolation level: READ COMMITTED (INSERT is write-only, no partition logic)
    -- Must be after all DECLARE statements
    SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;

    -- Initialize transaction logging
    SET current_transaction_id = UUID();
    SET @current_transaction_id = current_transaction_id;
    SET @current_log_sequence = 0;

    START TRANSACTION;

    SELECT AVG(averageRating) INTO global_mean
    FROM `stadvdb-mco2`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;

    SET @percentile := 0.95;
    SELECT COUNT(*) INTO @totalCount
    FROM `stadvdb-mco2`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;
    SET @rank_position := CEIL(@percentile * @totalCount);
    SET @rank_position := GREATEST(@rank_position, 1);
    
    SELECT numVotes INTO min_votes_threshold
    FROM (
        SELECT numVotes, ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
        FROM `stadvdb-mco2`.title_ft
        WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
    ) AS ordered_votes
    WHERE row_num = @rank_position
    LIMIT 1;

    IF new_numVotes IS NULL OR new_numVotes = 0 THEN
        SET calculated_weightedRating = global_mean;
    ELSE
        SET calculated_weightedRating = ROUND(
            (new_numVotes / (new_numVotes + min_votes_threshold)) * new_averageRating
            + (min_votes_threshold / (new_numVotes + min_votes_threshold)) * global_mean, 2
        );
    END IF;

    INSERT INTO `stadvdb-mco2`.title_ft
      (tconst, primaryTitle, runtimeMinutes,
       averageRating, numVotes, startYear, weightedRating)
    VALUES
      (new_tconst, new_primaryTitle, new_runtimeMinutes,
       new_averageRating, new_numVotes, new_startYear, calculated_weightedRating);

    -- Log INSERT to Main
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, table_name, record_id, operation_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'MODIFY', 'title_ft', new_tconst, 'INSERT', 'MAIN', NOW(6));

    -- Log COMMIT to Main
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'MAIN', NOW(6));

    -- COMMIT MAIN FIRST - this is the priority transaction
    COMMIT;

    -- Now attempt federated replication in a NEW transaction
    -- If it fails, Main already has the data
    START TRANSACTION;

    SET @federated_operation = 1;

    -- Try to replicate insert to remote node, but don't fail if node is down
    -- The procedure-level CONTINUE HANDLER will catch federated errors
    IF new_startYear IS NOT NULL AND new_startYear >= 2025 THEN
        -- Try to insert into Node A
        INSERT INTO title_ft_node_a
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, calculated_weightedRating, new_startYear);
        
        -- If federated operation failed, just skip the logging
        IF federated_error = 0 THEN
            CALL log_to_remote_node('NODE_A', @current_transaction_id, 2, 'MODIFY', 'title_ft', new_tconst, 'ALL_COLUMNS',
                NULL,
                JSON_OBJECT('tconst', new_tconst, 'primaryTitle', new_primaryTitle, 'runtimeMinutes', new_runtimeMinutes,
                           'averageRating', new_averageRating, 'numVotes', new_numVotes, 'weightedRating', calculated_weightedRating, 'startYear', new_startYear),
                'INSERT');
        END IF;
    ELSE
        -- Try to insert into Node B
        INSERT INTO title_ft_node_b
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, calculated_weightedRating, new_startYear);
        
        -- If federated operation failed, just skip the logging
        IF federated_error = 0 THEN
            CALL log_to_remote_node('NODE_B', @current_transaction_id, 2, 'MODIFY', 'title_ft', new_tconst, 'ALL_COLUMNS',
                NULL,
                JSON_OBJECT('tconst', new_tconst, 'primaryTitle', new_primaryTitle, 'runtimeMinutes', new_runtimeMinutes,
                           'averageRating', new_averageRating, 'numVotes', new_numVotes, 'weightedRating', calculated_weightedRating, 'startYear', new_startYear),
                'INSERT');
        END IF;
    END IF;

    -- Clear federated flag
    SET @federated_operation = NULL;

    -- Commit or rollback the federated transaction based on whether it succeeded
    IF federated_error = 1 THEN
        -- Federated failed, rollback this transaction (but Main insert already committed above)
        ROLLBACK;
    ELSE
        -- Federated succeeded, commit this transaction
        COMMIT;
    END IF;
    
    -- Clear session variables
    SET @current_transaction_id = NULL;
    SET @current_log_sequence = NULL;
END$$

DELIMITER ;

DELIMITER $$

CREATE PROCEDURE distributed_update(
    IN new_tconst VARCHAR(12),
    IN new_primaryTitle VARCHAR(1024),
    IN new_runtimeMinutes SMALLINT UNSIGNED,
    IN new_averageRating DECIMAL(3,1),
    IN new_numVotes INT UNSIGNED,
    IN new_startYear SMALLINT UNSIGNED
)


BEGIN
    DECLARE old_startYear SMALLINT UNSIGNED;
    DECLARE global_mean DECIMAL(3,1);
    DECLARE min_votes_threshold INT;
    DECLARE updated_weightedRating DECIMAL(4,2);
    DECLARE current_transaction_id VARCHAR(36);
    DECLARE federated_error INT DEFAULT 0;
    
    -- Handler for federated table errors - set flag and continue
    -- Error codes: 1429 (can't connect), 1158 (communication error), 1159 (net timeout), 
    --              1189 (net read timeout), 2013 (lost connection), 2006 (server gone),
    --              1296 (federated error wrapper), 1430 (query on foreign data source)
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
    BEGIN
        SET federated_error = 1;
        -- Main operation succeeds, federated replication failed
        -- Recovery system will sync when nodes come back online
    END;
    
    -- Set isolation level: REPEATABLE READ (must read startYear consistently for partition moves)
    -- Must be after all DECLARE statements
    SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;

    -- Initialize transaction logging
    SET current_transaction_id = UUID();
    SET @current_transaction_id = current_transaction_id;
    SET @current_log_sequence = 0;

    START TRANSACTION;

    -- GROWING PHASE: Lock the row immediately before reading
    SELECT startYear INTO old_startYear
    FROM `stadvdb-mco2`.title_ft
    WHERE tconst = new_tconst
    FOR UPDATE;  -- Acquire write lock for 2PL

    SELECT AVG(averageRating) INTO global_mean
    FROM `stadvdb-mco2`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;

    SET @percentile := 0.95;
    SELECT COUNT(*) INTO @totalCount
    FROM `stadvdb-mco2`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;
    SET @rank_position := CEIL(@percentile * @totalCount);
    SET @rank_position := GREATEST(@rank_position, 1);
    SELECT numVotes INTO min_votes_threshold
    FROM (
        SELECT numVotes, ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
        FROM `stadvdb-mco2`.title_ft
        WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
    ) AS ordered_votes
    WHERE row_num = @rank_position
    LIMIT 1;

    SET updated_weightedRating = ROUND(
        (new_numVotes / (new_numVotes + min_votes_threshold)) * new_averageRating
        + (min_votes_threshold / (new_numVotes + min_votes_threshold)) * global_mean, 2
    );

    -- UPDATE MAIN FIRST - this commit is more important than partition replication
    UPDATE `stadvdb-mco2`.title_ft
    SET primaryTitle = new_primaryTitle,
        runtimeMinutes = new_runtimeMinutes,
        averageRating = new_averageRating,
        numVotes = new_numVotes,
        startYear = new_startYear,
        weightedRating = updated_weightedRating
    WHERE tconst = new_tconst;

    -- Log UPDATE to Main
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, table_name, record_id, operation_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'MODIFY', 'title_ft', new_tconst, 'UPDATE', 'MAIN', NOW(6));

    -- Create a savepoint before attempting federated operations
    -- NOW attempt partition moves - if they fail, Main is already updated
    SAVEPOINT before_federated;

    -- Set flag to prevent triggers on federated nodes from logging
    SET @federated_operation = 1;

    -- Check if you need to move to a new node (use federated tables)
    -- >= 2025 = NODE_A, < 2025 (including NULL) = NODE_B
    -- These operations are "best effort" - if nodes are down, Main has already been updated
    IF (old_startYear IS NULL OR old_startYear < 2025) AND (new_startYear >= 2025) THEN
        -- Moving from B to A
        DELETE FROM title_ft_node_b WHERE tconst = new_tconst;
        
        -- If federated operation failed, just rollback to savepoint (Main already updated)
        IF federated_error = 1 THEN
            ROLLBACK TO SAVEPOINT before_federated;
        ELSE
            -- DELETE succeeded, now try INSERT
            INSERT INTO title_ft_node_a
            VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                    new_averageRating, new_numVotes, updated_weightedRating, new_startYear);
            
            -- If INSERT failed, rollback to savepoint
            IF federated_error = 1 THEN
                ROLLBACK TO SAVEPOINT before_federated;
            END IF;
        END IF;
        
    ELSEIF (old_startYear >= 2025) AND (new_startYear IS NULL OR new_startYear < 2025) THEN
        -- Moving from A to B
        DELETE FROM title_ft_node_a WHERE tconst = new_tconst;
        
        -- If federated operation failed, just rollback to savepoint (Main already updated)
        IF federated_error = 1 THEN
            ROLLBACK TO SAVEPOINT before_federated;
        END IF;
        
        -- Only attempt INSERT if DELETE succeeded
        IF federated_error = 0 THEN
            INSERT INTO title_ft_node_b
            VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                    new_averageRating, new_numVotes, updated_weightedRating, new_startYear);
            
            -- If INSERT failed, rollback to savepoint
            IF federated_error = 1 THEN
                ROLLBACK TO SAVEPOINT before_federated;
            END IF;
        END IF;
        
    ELSE
        -- Staying in the same node
        IF new_startYear IS NOT NULL AND new_startYear >= 2025 THEN
            UPDATE title_ft_node_a
            SET primaryTitle = new_primaryTitle,
                runtimeMinutes = new_runtimeMinutes,
                averageRating = new_averageRating,
                numVotes = new_numVotes,
                startYear = new_startYear,
                weightedRating = updated_weightedRating
            WHERE tconst = new_tconst;
            
            -- If UPDATE failed, rollback to savepoint
            IF federated_error = 1 THEN
                ROLLBACK TO SAVEPOINT before_federated;
            END IF;
        ELSE
            UPDATE title_ft_node_b
            SET primaryTitle = new_primaryTitle,
                runtimeMinutes = new_runtimeMinutes,
                averageRating = new_averageRating,
                numVotes = new_numVotes,
                startYear = new_startYear,
                weightedRating = updated_weightedRating
            WHERE tconst = new_tconst;
            
            -- If UPDATE failed, rollback to savepoint
            IF federated_error = 1 THEN
                ROLLBACK TO SAVEPOINT before_federated;
            END IF;
        END IF;
    END IF;

    -- Clear federated flag
    SET @federated_operation = NULL;
    
    -- Log COMMIT before committing transaction
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'MAIN', NOW(6));

    COMMIT;
    
    -- Clear session variables
    SET @current_transaction_id = NULL;
    SET @current_log_sequence = NULL;
END$$DELIMITER ;

DELIMITER $$

CREATE PROCEDURE distributed_delete(
    IN new_tconst VARCHAR(12)
)

BEGIN
    DECLARE current_transaction_id VARCHAR(36);
    DECLARE old_startYear SMALLINT UNSIGNED;
    DECLARE old_primaryTitle VARCHAR(1024);
    DECLARE old_runtimeMinutes SMALLINT UNSIGNED;
    DECLARE old_averageRating DECIMAL(3,1);
    DECLARE old_numVotes INT UNSIGNED;
    DECLARE old_weightedRating DECIMAL(10,2);
    DECLARE federated_error INT DEFAULT 0;
    DECLARE continue_federated INT DEFAULT 1;
    
    -- Handler for federated table errors - set flag and continue
    -- Error codes: 1429 (can't connect), 1158 (communication error), 1159 (net timeout), 
    --              1189 (net read timeout), 2013 (lost connection), 2006 (server gone),
    --              1296 (federated error wrapper), 1430 (query on foreign data source)
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
    BEGIN
        SET federated_error = 1;
        -- Main operation succeeds, federated replication failed
        -- Recovery system will sync when nodes come back online
    END;
    
    -- Set isolation level: SERIALIZABLE (must safely read all old values for logging before delete)
    -- Must be after all DECLARE statements
    SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    -- Initialize transaction logging
    SET current_transaction_id = UUID();
    SET @current_transaction_id = current_transaction_id;
    SET @current_log_sequence = 0;

    START TRANSACTION;

    -- GROWING PHASE: Lock the row immediately before reading all old values
    SELECT startYear, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating
    INTO old_startYear, old_primaryTitle, old_runtimeMinutes, old_averageRating, old_numVotes, old_weightedRating
    FROM `stadvdb-mco2`.title_ft
    WHERE tconst = new_tconst
    FOR UPDATE;  -- Acquire write lock for 2PL

    -- LOCKED WRITES PHASE: Delete from Main (lock still held)
    DELETE FROM `stadvdb-mco2`.title_ft
    WHERE tconst = new_tconst;

    -- Log DELETE to Main
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, table_name, record_id, operation_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'MODIFY', 'title_ft', new_tconst, 'DELETE', 'MAIN', NOW(6));

    -- Create a savepoint before attempting federated operations
    SAVEPOINT before_federated;

    -- Set flag to prevent triggers on federated nodes from logging
    SET @federated_operation = 1;

    -- Try to replicate delete to remote node, but don't fail if node is down
    -- Just attempt and ignore errors - Main delete already succeeded
    IF old_startYear IS NOT NULL AND old_startYear >= 2025 THEN
        BEGIN
            DECLARE EXIT HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
            BEGIN
                SET continue_federated = 0;
                ROLLBACK TO SAVEPOINT before_federated;
            END;
            
            IF continue_federated = 1 THEN
                DELETE FROM title_ft_node_a WHERE tconst = new_tconst;
                CALL log_to_remote_node('NODE_A', @current_transaction_id, 2, 'MODIFY', 'title_ft', new_tconst, 'ALL_COLUMNS',
                    JSON_OBJECT('tconst', new_tconst, 'primaryTitle', old_primaryTitle, 'runtimeMinutes', old_runtimeMinutes,
                               'averageRating', old_averageRating, 'numVotes', old_numVotes, 'weightedRating', old_weightedRating, 'startYear', old_startYear),
                    NULL, 'DELETE');
            END IF;
        END;
    ELSE
        BEGIN
            DECLARE EXIT HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
            BEGIN
                SET continue_federated = 0;
                ROLLBACK TO SAVEPOINT before_federated;
            END;
            
            IF continue_federated = 1 THEN
                DELETE FROM title_ft_node_b WHERE tconst = new_tconst;
                CALL log_to_remote_node('NODE_B', @current_transaction_id, 2, 'MODIFY', 'title_ft', new_tconst, 'ALL_COLUMNS',
                    JSON_OBJECT('tconst', new_tconst, 'primaryTitle', old_primaryTitle, 'runtimeMinutes', old_runtimeMinutes,
                               'averageRating', old_averageRating, 'numVotes', old_numVotes, 'weightedRating', old_weightedRating, 'startYear', old_startYear),
                    NULL, 'DELETE');
            END IF;
        END;
    END IF;

    -- Clear federated flag
    SET @federated_operation = NULL;

    -- Log COMMIT before committing transaction
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'MAIN', NOW(6));

    COMMIT;
    
    -- Clear session variables
    SET @current_transaction_id = NULL;
    SET @current_log_sequence = NULL;
END$$

DELIMITER ;

DELIMITER $$

CREATE PROCEDURE distributed_addReviews(
    IN new_tconst VARCHAR(12),
    IN num_new_reviews INT,
    IN new_rating DECIMAL(3,1)
)

BEGIN
    DECLARE current_numVotes INT UNSIGNED;
    DECLARE current_averageRating DECIMAL(3,1);
    DECLARE current_startYear SMALLINT UNSIGNED;

    DECLARE updated_numVotes INT UNSIGNED;
    DECLARE updated_averageRating DECIMAL(3,1);
    DECLARE updated_weightedRating DECIMAL(4,2);
    
    DECLARE global_mean DECIMAL(3,1);
    DECLARE min_votes_threshold INT;
    DECLARE current_transaction_id VARCHAR(36);
    DECLARE federated_error INT DEFAULT 0;
    
    -- Handler for federated table errors - set flag and continue
    -- Error codes: 1429 (can't connect), 1158 (communication error), 1159 (net timeout), 
    --              1189 (net read timeout), 2013 (lost connection), 2006 (server gone)
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
    BEGIN
        SET federated_error = 1;
        -- Main operation succeeds, federated replication failed
        -- Recovery system will sync when nodes come back online
    END;

    -- Validation AFTER all DECLARE statements
    IF num_new_reviews < 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'num_new_reviews cannot be negative';
    END IF;

    -- Initialize transaction logging
    SET current_transaction_id = UUID();
    SET @current_transaction_id = current_transaction_id;
    SET @current_log_sequence = 0;

    START TRANSACTION;

    SELECT numVotes, averageRating, startYear
    INTO current_numVotes, current_averageRating, current_startYear
    FROM `stadvdb-mco2`.title_ft
    WHERE tconst = new_tconst;

    SELECT AVG(averageRating) INTO global_mean
    FROM `stadvdb-mco2`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;

    SET @percentile := 0.95;
    SELECT COUNT(*) INTO @totalCount
    FROM `stadvdb-mco2`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;
    
    SET @rank_position := CEIL(@percentile * @totalCount);
    SET @rank_position := GREATEST(@rank_position, 1);
    
    SELECT numVotes INTO min_votes_threshold
    FROM (
        SELECT numVotes, ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
        FROM `stadvdb-mco2`.title_ft
        WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
    ) AS ordered_votes
    WHERE row_num = @rank_position
    LIMIT 1;


    SET updated_numVotes = current_numVotes + num_new_reviews;
    IF updated_numVotes < 0 THEN
        SET updated_numVotes = 0;
    END IF;

    IF updated_numVotes = 0 THEN
        SET updated_averageRating = NULL;
        SET updated_weightedRating = global_mean;
    ELSE
        SET updated_averageRating = ROUND(
            ((current_averageRating * current_numVotes) + (new_rating * num_new_reviews)) / updated_numVotes,
            1
        );
        -- Formula: (v / (v + m)) * r + (m / (v + m)) * g
        SET updated_weightedRating = ROUND(
            (updated_numVotes / (updated_numVotes + min_votes_threshold)) * updated_averageRating
            + (min_votes_threshold / (updated_numVotes + min_votes_threshold)) * global_mean, 2
        );
    END IF;

    UPDATE `stadvdb-mco2`.title_ft
    SET numVotes = updated_numVotes,
        averageRating = updated_averageRating,
        weightedRating = updated_weightedRating
    WHERE tconst = new_tconst;

    -- Log UPDATE to Main
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'MAIN', NOW(6));

    -- COMMIT MAIN FIRST - this is the priority transaction
    COMMIT;

    -- Now attempt federated replication in a NEW transaction
    -- If it fails, Main already has the data
    START TRANSACTION;

    -- Set flag to prevent cascade logging on remote nodes
    SET @federated_operation = 1;
    
    -- >= 2025 = NODE_A, < 2025 (including NULL) = NODE_B
    IF current_startYear IS NOT NULL AND current_startYear >= 2025 THEN
        UPDATE title_ft_node_a
        SET numVotes = updated_numVotes,
            averageRating = updated_averageRating,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
    ELSE
        UPDATE title_ft_node_b
        SET numVotes = updated_numVotes,
            averageRating = updated_averageRating,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
    END IF;
    
    -- Clear federated flag
    SET @federated_operation = NULL;

    -- Commit or rollback the federated transaction based on whether it succeeded
    IF federated_error = 1 THEN
        -- Federated failed, rollback this transaction (but Main update already committed above)
        ROLLBACK;
    ELSE
        -- Federated succeeded, commit this transaction
        COMMIT;
    END IF;
    
    -- Clear session variables
    SET @current_transaction_id = NULL;
    SET @current_log_sequence = NULL;
END$$

DELIMITER ;

DELIMITER $$

CREATE PROCEDURE distributed_select(
    IN select_column VARCHAR(50),
    IN order_direction VARCHAR(4),
    IN limit_count INT UNSIGNED
)

BEGIN
    IF order_direction NOT IN ('ASC', 'DESC') THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Order must be ASC or DESC';
    END IF;

    IF limit_count <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Limit count must be greater than 0';
    END IF;

    CASE select_column
        WHEN 'primaryTitle' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY primaryTitle ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY primaryTitle DESC
                LIMIT limit_count;
            END IF;

        WHEN 'numVotes' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY numVotes ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY numVotes DESC
                LIMIT limit_count;
            END IF;

        WHEN 'averageRating' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY averageRating ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY averageRating DESC
                LIMIT limit_count;
            END IF;

        WHEN 'weightedRating' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY weightedRating ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY weightedRating DESC
                LIMIT limit_count;
            END IF;

        WHEN 'startYear' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY startYear ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY startYear DESC
                LIMIT limit_count;
            END IF;

        WHEN 'tconst' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY tconst ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY tconst DESC
                LIMIT limit_count;
            END IF;

        WHEN 'runtimeMinutes' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY runtimeMinutes ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM `stadvdb-mco2`.title_ft
                ORDER BY runtimeMinutes DESC
                LIMIT limit_count;
            END IF;

        ELSE
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Invalid column';
    END CASE;

END$$

DELIMITER ;

DELIMITER $$

CREATE PROCEDURE distributed_search(
    IN search_term VARCHAR(1024),
    IN limit_count INT UNSIGNED
)

BEGIN
    IF search_term IS NULL OR search_term = '' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Empty search';
    END IF;

    IF limit_count IS NULL OR limit_count <= 0 THEN
        SET limit_count = 20;
    END IF;

    SELECT * FROM `stadvdb-mco2`.title_ft
    WHERE primaryTitle LIKE CONCAT('%', search_term, '%')
    ORDER BY weightedRating DESC
    LIMIT limit_count;

END$$

DELIMITER ;

DELIMITER $$

CREATE PROCEDURE distributed_aggregation()

BEGIN
    SELECT 
        COUNT(*) AS movie_count,
        AVG(averageRating) AS average_rating,
        AVG(weightedRating) AS average_weightedRating,
        SUM(numVotes) AS total_votes,
        AVG(numVotes) AS average_votes
    FROM `stadvdb-mco2`.title_ft;

END$$

DELIMITER ;
