-- This file contains just the distributed_update procedure with debug statements
-- Run this on MAIN database to install the debug version

USE `stadvdb-mco2`;

DROP PROCEDURE IF EXISTS distributed_update;

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

    -- Initialize transaction logging
    SET current_transaction_id = UUID();
    SET @current_transaction_id = current_transaction_id;
    SET @current_log_sequence = 0;

    START TRANSACTION;

    -- DEBUG
    SELECT CONCAT('DEBUG: Starting update for tconst=', new_tconst, ', newYear=', new_startYear) AS debug_msg;
    SELECT CONCAT('DEBUG: Procedure parameters - title=', new_primaryTitle, ', year=', new_startYear) AS debug_msg;

    -- Get initial startYear
    SELECT startYear INTO old_startYear
    FROM `stadvdb-mco2`.title_ft
    WHERE tconst = new_tconst;

    -- DEBUG
    SELECT CONCAT('DEBUG: Found old startYear=', IFNULL(old_startYear, 'NULL')) AS debug_msg;

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

    -- DEBUG
    SELECT CONCAT('DEBUG: Updated Main table. Rows affected query should show above') AS debug_msg;
    SELECT CONCAT('DEBUG: Current Main data - tconst=', tconst, ', startYear=', startYear) AS debug_msg
    FROM `stadvdb-mco2`.title_ft WHERE tconst = new_tconst LIMIT 1;

    -- Log UPDATE to Main
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, table_name, record_id, operation_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'MODIFY', 'title_ft', new_tconst, 'UPDATE', 'MAIN', NOW(6));

    -- Create a savepoint before attempting federated operations
    -- NOW attempt partition moves - if they fail, Main is already updated
    SAVEPOINT before_federated;

    -- DEBUG
    SELECT CONCAT('DEBUG: Created savepoint. old_startYear=', IFNULL(old_startYear, 'NULL'), ', new_startYear=', new_startYear) AS debug_msg;

    -- Set flag to prevent triggers on federated nodes from logging
    SET @federated_operation = 1;

    -- Check if you need to move to a new node (use federated tables)
    -- >= 2025 = NODE_A, < 2025 (including NULL) = NODE_B
    -- These operations are "best effort" - if nodes are down, Main has already been updated
    IF (old_startYear IS NULL OR old_startYear < 2025) AND (new_startYear >= 2025) THEN
        -- Moving from B to A
        SELECT CONCAT('DEBUG: Moving from B to A. Deleting from Node B') AS debug_msg;
        DELETE FROM title_ft_node_b WHERE tconst = new_tconst;
        
        -- DEBUG
        SELECT CONCAT('DEBUG: After Node B delete - federated_error=', federated_error) AS debug_msg;
        
        -- If federated operation failed, just rollback to savepoint (Main already updated)
        IF federated_error = 1 THEN
            SELECT CONCAT('DEBUG: Node B delete failed, rolling back to savepoint') AS debug_msg;
            ROLLBACK TO SAVEPOINT before_federated;
        ELSE
            -- DELETE succeeded, now try INSERT
            SELECT CONCAT('DEBUG: Node B delete succeeded, now inserting to Node A') AS debug_msg;
            INSERT INTO title_ft_node_a
            VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                    new_averageRating, new_numVotes, updated_weightedRating, new_startYear);
            
            -- DEBUG
            SELECT CONCAT('DEBUG: After Node A insert - federated_error=', federated_error) AS debug_msg;
            
            -- If INSERT failed, rollback to savepoint
            IF federated_error = 1 THEN
                SELECT CONCAT('DEBUG: Node A insert failed, rolling back to savepoint') AS debug_msg;
                ROLLBACK TO SAVEPOINT before_federated;
            END IF;
        END IF;
        
    ELSEIF (old_startYear >= 2025) AND (new_startYear IS NULL OR new_startYear < 2025) THEN
        -- Moving from A to B
        DELETE FROM title_ft_node_a WHERE tconst = new_tconst;
        
        -- If federated operation failed, just rollback to savepoint (Main already updated)
        IF federated_error = 1 THEN
            ROLLBACK TO SAVEPOINT before_federated;
        ELSE
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

    -- DEBUG
    SELECT CONCAT('DEBUG: About to commit transaction') AS debug_msg;

    -- Log COMMIT before committing transaction
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'MAIN', NOW(6));

    COMMIT;
    
    -- DEBUG
    SELECT CONCAT('DEBUG: Transaction committed successfully') AS debug_msg;
    
    -- Clear session variables
    SET @current_transaction_id = NULL;
    SET @current_log_sequence = NULL;
END$$

DELIMITER ;

-- Now test it manually
SELECT '===== BEFORE UPDATE =====' AS test;
SELECT tconst, startYear FROM `stadvdb-mco2`.title_ft WHERE tconst='tt35269191' LIMIT 1;

SELECT '===== CALLING PROCEDURE =====' AS test;
CALL distributed_update('tt35269191', 'Test Title', 120, 8.5, 1000, 2025);

SELECT '===== AFTER UPDATE =====' AS test;
SELECT tconst, startYear FROM `stadvdb-mco2`.title_ft WHERE tconst='tt35269191' LIMIT 1;
