-- ============================================================================
-- NODE A DISTRIBUTED PROCEDURES WITH AUTOMATIC FAILOVER
-- Mirrors Main's procedures but only activates when in ACTING_MASTER mode
-- Automatically handles failover when Main is unreachable
-- ============================================================================

USE `stadvdb-mco2-a`;

DROP PROCEDURE IF EXISTS distributed_insert;
DROP PROCEDURE IF EXISTS distributed_update;
DROP PROCEDURE IF EXISTS distributed_delete;
DROP PROCEDURE IF EXISTS distributed_addReviews;

DELIMITER $$

-- ============================================================================
-- DISTRIBUTED INSERT - WITH FAILOVER SUPPORT
-- ============================================================================

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
    DECLARE current_mode VARCHAR(20);

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Log ABORT before rolling back
        IF @current_transaction_id IS NOT NULL THEN
            SET @current_log_sequence = IFNULL(@current_log_sequence, 0) + 1;
            INSERT INTO transaction_log 
            (transaction_id, log_sequence, log_type, source_node, timestamp)
            VALUES (@current_transaction_id, @current_log_sequence, 'ABORT', 'NODE_A_ACTING', NOW(6));
        END IF;
        
        -- Clear session variables
        SET @current_transaction_id = NULL;
        SET @current_log_sequence = NULL;
        
        ROLLBACK;
        RESIGNAL;
    END;

    -- Check current node mode
    SELECT config_value INTO current_mode 
    FROM node_config 
    WHERE config_key = 'node_mode';
    
    -- Only allow writes if in ACTING_MASTER mode
    IF current_mode != 'ACTING_MASTER' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Node A is in VICE mode. Please use Main node for writes.';
    END IF;

    -- Initialize transaction logging
    SET current_transaction_id = UUID();
    SET @current_transaction_id = current_transaction_id;
    SET @current_log_sequence = 0;

    START TRANSACTION;

    -- Calculate global mean from local data
    SELECT AVG(averageRating) INTO global_mean
    FROM `stadvdb-mco2-a`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;

    SET @percentile := 0.95;
    SELECT COUNT(*) INTO @totalCount
    FROM `stadvdb-mco2-a`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;
    SET @rank_position := CEIL(@percentile * @totalCount);
    SET @rank_position := GREATEST(@rank_position, 1);
    
    SELECT numVotes INTO min_votes_threshold
    FROM (
        SELECT numVotes, ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
        FROM `stadvdb-mco2-a`.title_ft
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

    -- Insert into local Node A table
    INSERT INTO `stadvdb-mco2-a`.title_ft
      (tconst, primaryTitle, runtimeMinutes,
       averageRating, numVotes, startYear, weightedRating)
    VALUES
      (new_tconst, new_primaryTitle, new_runtimeMinutes,
       new_averageRating, new_numVotes, new_startYear, calculated_weightedRating);

    -- Set flag to prevent triggers on federated nodes from logging
    SET @federated_operation = 1;

    -- Try to replicate to Main (if it's back online)
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            -- Main is still down, skip replication to Main
            SELECT 'Warning: Could not replicate to Main (still down)' AS warning;
        END;
        
        INSERT INTO title_ft_main
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, calculated_weightedRating, new_startYear);
    END;

    -- Replicate to Node B
    -- >= 2025 = stays in NODE_A, < 2025 (including NULL) = goes to NODE_B
    IF new_startYear IS NULL OR new_startYear < 2025 THEN
        INSERT INTO title_ft_node_b
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, calculated_weightedRating, new_startYear);
    END IF;

    -- Clear federated flag
    SET @federated_operation = NULL;

    -- Log COMMIT before committing transaction
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'NODE_A_ACTING', NOW(6));

    COMMIT;
    
    -- Clear session variables
    SET @current_transaction_id = NULL;
    SET @current_log_sequence = NULL;
END$$

-- ============================================================================
-- DISTRIBUTED UPDATE - WITH FAILOVER SUPPORT
-- ============================================================================

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
    DECLARE current_mode VARCHAR(20);

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        IF @current_transaction_id IS NOT NULL THEN
            SET @current_log_sequence = IFNULL(@current_log_sequence, 0) + 1;
            INSERT INTO transaction_log 
            (transaction_id, log_sequence, log_type, source_node, timestamp)
            VALUES (@current_transaction_id, @current_log_sequence, 'ABORT', 'NODE_A_ACTING', NOW(6));
        END IF;
        
        SET @current_transaction_id = NULL;
        SET @current_log_sequence = NULL;
        
        ROLLBACK;
        RESIGNAL;
    END;

    -- Check current node mode
    SELECT config_value INTO current_mode 
    FROM node_config 
    WHERE config_key = 'node_mode';
    
    IF current_mode != 'ACTING_MASTER' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Node A is in VICE mode. Please use Main node for writes.';
    END IF;

    -- Initialize transaction logging
    SET current_transaction_id = UUID();
    SET @current_transaction_id = current_transaction_id;
    SET @current_log_sequence = 0;

    START TRANSACTION;

    -- Get initial startYear
    SELECT startYear INTO old_startYear
    FROM `stadvdb-mco2-a`.title_ft
    WHERE tconst = new_tconst;

    SELECT AVG(averageRating) INTO global_mean
    FROM `stadvdb-mco2-a`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;

    SET @percentile := 0.95;
    SELECT COUNT(*) INTO @totalCount
    FROM `stadvdb-mco2-a`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;
    SET @rank_position := CEIL(@percentile * @totalCount);
    SET @rank_position := GREATEST(@rank_position, 1);
    
    SELECT numVotes INTO min_votes_threshold
    FROM (
        SELECT numVotes, ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
        FROM `stadvdb-mco2-a`.title_ft
        WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
    ) AS ordered_votes
    WHERE row_num = @rank_position
    LIMIT 1;

    SET updated_weightedRating = ROUND(
        (new_numVotes / (new_numVotes + min_votes_threshold)) * new_averageRating
        + (min_votes_threshold / (new_numVotes + min_votes_threshold)) * global_mean, 2
    );

    -- Update local Node A table
    UPDATE `stadvdb-mco2-a`.title_ft
    SET primaryTitle = new_primaryTitle,
        runtimeMinutes = new_runtimeMinutes,
        averageRating = new_averageRating,
        numVotes = new_numVotes,
        startYear = new_startYear,
        weightedRating = updated_weightedRating
    WHERE tconst = new_tconst;

    SET @federated_operation = 1;

    -- Try to replicate to Main (if it's back online)
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            SELECT 'Warning: Could not replicate to Main (still down)' AS warning;
        END;
        
        UPDATE title_ft_main
        SET primaryTitle = new_primaryTitle,
            runtimeMinutes = new_runtimeMinutes,
            averageRating = new_averageRating,
            numVotes = new_numVotes,
            startYear = new_startYear,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
    END;

    -- Handle node migration if startYear changed
    IF (old_startYear IS NULL OR old_startYear < 2025) AND (new_startYear >= 2025) THEN
        -- Moving from B to A - delete from B
        DELETE FROM title_ft_node_b WHERE tconst = new_tconst;
    ELSEIF (old_startYear >= 2025) AND (new_startYear IS NULL OR new_startYear < 2025) THEN
        -- Moving from A to B - insert into B
        INSERT INTO title_ft_node_b
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, updated_weightedRating, new_startYear);
    ELSE
        -- Staying in same partition - update Node B if record is there
        IF new_startYear IS NULL OR new_startYear < 2025 THEN
            UPDATE title_ft_node_b
            SET primaryTitle = new_primaryTitle,
                runtimeMinutes = new_runtimeMinutes,
                averageRating = new_averageRating,
                numVotes = new_numVotes,
                startYear = new_startYear,
                weightedRating = updated_weightedRating
            WHERE tconst = new_tconst;
        END IF;
    END IF;

    SET @federated_operation = NULL;

    -- Log COMMIT
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'NODE_A_ACTING', NOW(6));

    COMMIT;
    
    SET @current_transaction_id = NULL;
    SET @current_log_sequence = NULL;
END$$

-- ============================================================================
-- DISTRIBUTED DELETE - WITH FAILOVER SUPPORT
-- ============================================================================

CREATE PROCEDURE distributed_delete(
    IN new_tconst VARCHAR(12)
)
BEGIN
    DECLARE current_transaction_id VARCHAR(36);
    DECLARE current_mode VARCHAR(20);
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        IF @current_transaction_id IS NOT NULL THEN
            SET @current_log_sequence = IFNULL(@current_log_sequence, 0) + 1;
            INSERT INTO transaction_log 
            (transaction_id, log_sequence, log_type, source_node, timestamp)
            VALUES (@current_transaction_id, @current_log_sequence, 'ABORT', 'NODE_A_ACTING', NOW(6));
        END IF;
        
        SET @current_transaction_id = NULL;
        SET @current_log_sequence = NULL;
        
        ROLLBACK;
        RESIGNAL;
    END;

    -- Check current node mode
    SELECT config_value INTO current_mode 
    FROM node_config 
    WHERE config_key = 'node_mode';
    
    IF current_mode != 'ACTING_MASTER' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Node A is in VICE mode. Please use Main node for writes.';
    END IF;

    -- Initialize transaction logging
    SET current_transaction_id = UUID();
    SET @current_transaction_id = current_transaction_id;
    SET @current_log_sequence = 0;

    START TRANSACTION;

    -- Delete from local Node A table
    DELETE FROM `stadvdb-mco2-a`.title_ft
    WHERE tconst = new_tconst;

    SET @federated_operation = 1;

    -- Try to replicate to Main (if it's back online)
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            SELECT 'Warning: Could not replicate to Main (still down)' AS warning;
        END;
        
        DELETE FROM title_ft_main WHERE tconst = new_tconst;
    END;

    -- Delete from Node B
    DELETE FROM title_ft_node_b WHERE tconst = new_tconst;

    SET @federated_operation = NULL;

    -- Log COMMIT
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'NODE_A_ACTING', NOW(6));

    COMMIT;
    
    SET @current_transaction_id = NULL;
    SET @current_log_sequence = NULL;
END$$

-- ============================================================================
-- DISTRIBUTED ADD REVIEWS - WITH FAILOVER SUPPORT
-- ============================================================================

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
    DECLARE current_mode VARCHAR(20);
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        IF @current_transaction_id IS NOT NULL THEN
            SET @current_log_sequence = IFNULL(@current_log_sequence, 0) + 1;
            INSERT INTO transaction_log 
            (transaction_id, log_sequence, log_type, source_node, timestamp)
            VALUES (@current_transaction_id, @current_log_sequence, 'ABORT', 'NODE_A_ACTING', NOW(6));
        END IF;
        
        SET @current_transaction_id = NULL;
        SET @current_log_sequence = NULL;
        
        ROLLBACK;
        RESIGNAL;
    END;

    -- Validation
    IF num_new_reviews < 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'num_new_reviews cannot be negative';
    END IF;

    -- Check current node mode
    SELECT config_value INTO current_mode 
    FROM node_config 
    WHERE config_key = 'node_mode';
    
    IF current_mode != 'ACTING_MASTER' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Node A is in VICE mode. Please use Main node for writes.';
    END IF;

    -- Initialize transaction logging
    SET current_transaction_id = UUID();
    SET @current_transaction_id = current_transaction_id;
    SET @current_log_sequence = 0;

    START TRANSACTION;

    SELECT numVotes, averageRating, startYear
    INTO current_numVotes, current_averageRating, current_startYear
    FROM `stadvdb-mco2-a`.title_ft
    WHERE tconst = new_tconst;

    SELECT AVG(averageRating) INTO global_mean
    FROM `stadvdb-mco2-a`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;

    SET @percentile := 0.95;
    SELECT COUNT(*) INTO @totalCount
    FROM `stadvdb-mco2-a`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;
    
    SET @rank_position := CEIL(@percentile * @totalCount);
    SET @rank_position := GREATEST(@rank_position, 1);
    
    SELECT numVotes INTO min_votes_threshold
    FROM (
        SELECT numVotes, ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
        FROM `stadvdb-mco2-a`.title_ft
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
        SET updated_weightedRating = ROUND(
            (updated_numVotes / (updated_numVotes + min_votes_threshold)) * updated_averageRating
            + (min_votes_threshold / (updated_numVotes + min_votes_threshold)) * global_mean, 2
        );
    END IF;

    -- Update local Node A table
    UPDATE `stadvdb-mco2-a`.title_ft
    SET numVotes = updated_numVotes,
        averageRating = updated_averageRating,
        weightedRating = updated_weightedRating
    WHERE tconst = new_tconst;

    SET @federated_operation = 1;
    
    -- Try to replicate to Main (if it's back online)
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            SELECT 'Warning: Could not replicate to Main (still down)' AS warning;
        END;
        
        UPDATE title_ft_main
        SET numVotes = updated_numVotes,
            averageRating = updated_averageRating,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
    END;

    -- Update Node B if record is there
    IF current_startYear IS NULL OR current_startYear < 2025 THEN
        UPDATE title_ft_node_b
        SET numVotes = updated_numVotes,
            averageRating = updated_averageRating,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
    END IF;
    
    SET @federated_operation = NULL;

    -- Log COMMIT
    SET @current_log_sequence = @current_log_sequence + 1;
    INSERT INTO transaction_log 
    (transaction_id, log_sequence, log_type, source_node, timestamp)
    VALUES (@current_transaction_id, @current_log_sequence, 'COMMIT', 'NODE_A_ACTING', NOW(6));

    COMMIT;
    
    SET @current_transaction_id = NULL;
    SET @current_log_sequence = NULL;
END$$

DELIMITER ;

SELECT 'Node A distributed procedures with failover support created successfully' AS status;
