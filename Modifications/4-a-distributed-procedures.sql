-- ============================================================================
-- DISTRIBUTED PROCEDURES FOR NODE A (Secondary Coordinator)
-- This file adds distributed procedures to Node A so it can serve as the
-- acting coordinator when Main is offline.
-- ============================================================================

USE `stadvdb-mco2-a`;

DROP PROCEDURE IF EXISTS distributed_insert;
DROP PROCEDURE IF EXISTS distributed_update;
DROP PROCEDURE IF EXISTS distributed_delete;
DROP PROCEDURE IF EXISTS distributed_addReviews;
DROP PROCEDURE IF EXISTS distributed_select;
DROP PROCEDURE IF EXISTS distributed_search;
DROP PROCEDURE IF EXISTS distributed_aggregation;

DELIMITER $$

-- ============================================================================
-- DISTRIBUTED AGGREGATION
-- Aggregates data across Node A and Node B when Main is offline
-- ============================================================================

CREATE PROCEDURE distributed_aggregation()
BEGIN
    -- Handler for federated table errors - include local data only if remote fails
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
    BEGIN
        -- Node B is down, continue with Node A data only
    END;
    
    -- Try to aggregate from both nodes
    -- Use UNION ALL to combine local title_ft with federated title_ft_node_b
    SELECT 
        COUNT(*) AS movie_count,
        AVG(averageRating) AS average_rating,
        AVG(weightedRating) AS average_weightedRating,
        SUM(numVotes) AS total_votes,
        AVG(numVotes) AS average_votes
    FROM (
        SELECT tconst, averageRating, weightedRating, numVotes
        FROM title_ft
        UNION ALL
        SELECT tconst, averageRating, weightedRating, numVotes
        FROM title_ft_node_b
    ) AS combined_data;
END$$

DELIMITER ;

DELIMITER $$

-- ============================================================================
-- DISTRIBUTED SELECT
-- Selects and orders data across Node A and Node B when Main is offline
-- ============================================================================

CREATE PROCEDURE distributed_select(
    IN select_column VARCHAR(50),
    IN order_direction VARCHAR(4),
    IN limit_count INT UNSIGNED
)
BEGIN
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
    BEGIN
        -- Node B is down, continue with Node A data only
    END;

    IF order_direction NOT IN ('ASC', 'DESC') THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Order must be ASC or DESC';
    END IF;

    IF limit_count <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Limit count must be greater than 0';
    END IF;

    -- Combine data from both nodes and apply ordering/limit
    CASE select_column
        WHEN 'primaryTitle' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY primaryTitle ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY primaryTitle DESC
                LIMIT limit_count;
            END IF;

        WHEN 'numVotes' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY numVotes ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY numVotes DESC
                LIMIT limit_count;
            END IF;

        WHEN 'averageRating' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY averageRating ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY averageRating DESC
                LIMIT limit_count;
            END IF;

        WHEN 'weightedRating' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY weightedRating ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY weightedRating DESC
                LIMIT limit_count;
            END IF;

        WHEN 'startYear' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY startYear ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY startYear DESC
                LIMIT limit_count;
            END IF;

        WHEN 'tconst' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY tconst ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY tconst DESC
                LIMIT limit_count;
            END IF;

        WHEN 'runtimeMinutes' THEN
            IF order_direction = 'ASC' THEN
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
                ORDER BY runtimeMinutes ASC
                LIMIT limit_count;
            ELSE
                SELECT * FROM (
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft
                    UNION ALL
                    SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
                    FROM title_ft_node_b
                ) AS combined
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

-- ============================================================================
-- DISTRIBUTED SEARCH
-- Searches across Node A and Node B when Main is offline
-- ============================================================================

CREATE PROCEDURE distributed_search(
    IN search_term VARCHAR(1024),
    IN limit_count INT UNSIGNED
)
BEGIN
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
    BEGIN
        -- Node B is down, continue with Node A data only
    END;

    IF search_term IS NULL OR search_term = '' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Empty search';
    END IF;

    IF limit_count IS NULL OR limit_count <= 0 THEN
        SET limit_count = 20;
    END IF;

    -- Search across both nodes
    SELECT * FROM (
        SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
        FROM title_ft
        WHERE primaryTitle LIKE CONCAT('%', search_term, '%')
        UNION ALL
        SELECT tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear
        FROM title_ft_node_b
        WHERE primaryTitle LIKE CONCAT('%', search_term, '%')
    ) AS combined
    ORDER BY weightedRating DESC
    LIMIT limit_count;
END$$

DELIMITER ;

DELIMITER $$

-- ============================================================================
-- DISTRIBUTED INSERT
-- Inserts data to appropriate node (A or B) when Main is offline
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
    DECLARE federated_error INT DEFAULT 0;
    DECLARE result_message VARCHAR(500);
    
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
    BEGIN
        SET federated_error = 1;
    END;

    -- PHASE 1: Insert to appropriate node based on startYear (guaranteed)
    -- Use the provided averageRating as weightedRating (no complex calculation)
    IF new_startYear IS NOT NULL AND new_startYear >= 2025 THEN
        -- Insert to Node A (local)
        START TRANSACTION;
        INSERT INTO title_ft
        (tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear, weightedRating)
        VALUES
        (new_tconst, new_primaryTitle, new_runtimeMinutes, new_averageRating, new_numVotes, new_startYear, new_averageRating);
        
        INSERT INTO transaction_log (transaction_id, log_sequence, log_type, operation_type, record_id, new_value, timestamp, source_node)
        VALUES (UUID(), 1, 'COMMIT', 'INSERT', new_tconst, 
                JSON_OBJECT('tconst', new_tconst, 'primaryTitle', new_primaryTitle, 'runtimeMinutes', new_runtimeMinutes, 
                           'averageRating', new_averageRating, 'numVotes', new_numVotes, 'startYear', new_startYear, 'weightedRating', new_averageRating),
                NOW(6), 'NODE_A');
        COMMIT;
    ELSE
        -- Insert to Node B (local)
        START TRANSACTION;
        INSERT INTO title_ft_node_b
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes, new_averageRating, new_numVotes, new_averageRating, new_startYear);
        
        INSERT INTO transaction_log (transaction_id, log_sequence, log_type, operation_type, record_id, new_value, timestamp, source_node)
        VALUES (UUID(), 1, 'COMMIT', 'INSERT', new_tconst, 
                JSON_OBJECT('tconst', new_tconst, 'primaryTitle', new_primaryTitle, 'runtimeMinutes', new_runtimeMinutes, 
                           'averageRating', new_averageRating, 'numVotes', new_numVotes, 'startYear', new_startYear, 'weightedRating', new_averageRating),
                NOW(6), 'NODE_A');
        COMMIT;
    END IF;

    -- PHASE 2: Attempt replication to Main (best effort, non-blocking)
    -- This is best-effort and should NOT fail the entire procedure if Main is down
    SET federated_error = 0;
    
    BEGIN
        DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430, 1317, 3024, SQLEXCEPTION
        BEGIN
            SET federated_error = 1;
        END;
        
        -- Set a short timeout (10 seconds max)
        SET SESSION max_execution_time = 10000;
        
        START TRANSACTION;
        INSERT INTO title_ft_main
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes, new_averageRating, new_numVotes, new_averageRating, new_startYear);
        COMMIT;
        
        SET SESSION max_execution_time = 0;
    END;
    
    -- Set result message (only ONE SELECT at the end)
    IF federated_error = 0 THEN
        SET result_message = CONCAT('✅ Inserted to Node A/B and replicated to Main: ', new_tconst);
    ELSE
        SET result_message = CONCAT('⚠️ Inserted to Node A/B but Main is unreachable (will recover later): ', new_tconst);
    END IF;
    
    -- Return single result set
    SELECT result_message AS result;
END$$

DELIMITER ;

DELIMITER $$

-- ============================================================================
-- DISTRIBUTED UPDATE
-- Updates data on appropriate node when Main is offline
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
    DECLARE federated_error INT DEFAULT 0;
    DECLARE found_in_a BOOLEAN DEFAULT FALSE;
    
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
    BEGIN
        SET federated_error = 1;
    END;

    -- Check if record exists in Node A
    SELECT COUNT(*) > 0, MAX(startYear) INTO found_in_a, old_startYear
    FROM title_ft
    WHERE tconst = new_tconst;

    IF NOT found_in_a THEN
        -- Try to find in Node B
        SELECT startYear INTO old_startYear
        FROM title_ft_node_b
        WHERE tconst = new_tconst;
    END IF;

    -- Calculate global mean
    SELECT AVG(averageRating) INTO global_mean
    FROM (
        SELECT averageRating FROM title_ft WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
        UNION ALL
        SELECT averageRating FROM title_ft_node_b WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
    ) AS combined;

    -- Calculate threshold
    SET @percentile := 0.95;
    SELECT COUNT(*) INTO @totalCount
    FROM (
        SELECT numVotes FROM title_ft WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
        UNION ALL
        SELECT numVotes FROM title_ft_node_b WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
    ) AS combined;
    
    SET @rank_position := CEIL(@percentile * @totalCount);
    SET @rank_position := GREATEST(@rank_position, 1);
    
    SELECT numVotes INTO min_votes_threshold
    FROM (
        SELECT numVotes, ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
        FROM (
            SELECT numVotes FROM title_ft WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
            UNION ALL
            SELECT numVotes FROM title_ft_node_b WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
        ) AS all_votes
    ) AS ordered_votes
    WHERE row_num = @rank_position
    LIMIT 1;

    SET updated_weightedRating = ROUND(
        (new_numVotes / (new_numVotes + min_votes_threshold)) * new_averageRating
        + (min_votes_threshold / (new_numVotes + min_votes_threshold)) * global_mean, 2
    );

    -- Handle partition changes
    IF (old_startYear IS NULL OR old_startYear < 2025) AND (new_startYear >= 2025) THEN
        -- Moving from B to A
        DELETE FROM title_ft_node_b WHERE tconst = new_tconst;
        INSERT INTO title_ft
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes, new_averageRating, new_numVotes, updated_weightedRating, new_startYear);
        
    ELSEIF (old_startYear >= 2025) AND (new_startYear IS NULL OR new_startYear < 2025) THEN
        -- Moving from A to B
        DELETE FROM title_ft WHERE tconst = new_tconst;
        INSERT INTO title_ft_node_b
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes, new_averageRating, new_numVotes, updated_weightedRating, new_startYear);
        
    ELSE
        -- Staying in same node
        IF new_startYear IS NOT NULL AND new_startYear >= 2025 THEN
            -- Update in Node A
            UPDATE title_ft
            SET primaryTitle = new_primaryTitle,
                runtimeMinutes = new_runtimeMinutes,
                averageRating = new_averageRating,
                numVotes = new_numVotes,
                startYear = new_startYear,
                weightedRating = updated_weightedRating
            WHERE tconst = new_tconst;
        ELSE
            -- Update in Node B
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
END$$

DELIMITER ;

DELIMITER $$

-- ============================================================================
-- DISTRIBUTED DELETE
-- Deletes data from appropriate node when Main is offline
-- ============================================================================

CREATE PROCEDURE distributed_delete(
    IN new_tconst VARCHAR(12)
)
BEGIN
    DECLARE old_startYear SMALLINT UNSIGNED;
    DECLARE old_value_json TEXT;
    DECLARE federated_error INT DEFAULT 0;
    DECLARE found_in_a BOOLEAN DEFAULT FALSE;
    
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
    BEGIN
        SET federated_error = 1;
    END;

    -- Check if record exists in Node A
    SELECT COUNT(*) > 0, MAX(startYear) INTO found_in_a, old_startYear
    FROM title_ft
    WHERE tconst = new_tconst;

    -- PHASE 1: Delete from appropriate node (guaranteed)
    IF found_in_a THEN
        -- Delete from Node A (local)
        START TRANSACTION;
        SELECT JSON_OBJECT('tconst', tconst, 'startYear', startYear) INTO old_value_json
        FROM title_ft WHERE tconst = new_tconst LIMIT 1;
        
        DELETE FROM title_ft WHERE tconst = new_tconst;
        
        INSERT INTO transaction_log (transaction_id, log_sequence, log_type, operation_type, record_id, old_value, timestamp, source_node)
        VALUES (UUID(), 1, 'COMMIT', 'DELETE', new_tconst, old_value_json, NOW(6), 'NODE_A');
        COMMIT;
    ELSE
        -- Delete from Node B (federated)
        START TRANSACTION;
        SELECT JSON_OBJECT('tconst', tconst, 'startYear', startYear) INTO old_value_json
        FROM title_ft_node_b WHERE tconst = new_tconst LIMIT 1;
        
        DELETE FROM title_ft_node_b WHERE tconst = new_tconst;
        
        INSERT INTO transaction_log (transaction_id, log_sequence, log_type, operation_type, record_id, old_value, timestamp, source_node)
        VALUES (UUID(), 1, 'COMMIT', 'DELETE', new_tconst, old_value_json, NOW(6), 'NODE_A');
        COMMIT;
    END IF;

    -- PHASE 2: Attempt replication to Main (best effort)
    SET federated_error = 0;
    START TRANSACTION;
    BEGIN
        DECLARE EXIT HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
        BEGIN
            SET federated_error = 1;
            ROLLBACK;
        END;
        
        DELETE FROM title_ft_main WHERE tconst = new_tconst;
        COMMIT;
    END;
    
    IF federated_error = 0 THEN
        SELECT CONCAT('✅ Deleted from Node A/B and replicated to Main: ', new_tconst) AS result;
    ELSE
        SELECT CONCAT('⚠️ Deleted from Node A/B but Main is unreachable (will recover later): ', new_tconst) AS result;
    END IF;
END$$

DELIMITER ;

DELIMITER $$

-- ============================================================================
-- DISTRIBUTED ADD REVIEWS
-- Adds reviews to appropriate node when Main is offline
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
    DECLARE federated_error INT DEFAULT 0;
    DECLARE found_in_a BOOLEAN DEFAULT FALSE;
    
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
    BEGIN
        SET federated_error = 1;
    END;

    IF num_new_reviews < 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'num_new_reviews cannot be negative';
    END IF;

    -- Find the record in Node A or B
    SELECT COUNT(*) > 0, MAX(numVotes), MAX(averageRating), MAX(startYear) 
    INTO found_in_a, current_numVotes, current_averageRating, current_startYear
    FROM title_ft
    WHERE tconst = new_tconst;

    IF NOT found_in_a THEN
        SELECT numVotes, averageRating, startYear
        INTO current_numVotes, current_averageRating, current_startYear
        FROM title_ft_node_b
        WHERE tconst = new_tconst;
    END IF;

    -- Calculate global mean
    SELECT AVG(averageRating) INTO global_mean
    FROM (
        SELECT averageRating FROM title_ft WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
        UNION ALL
        SELECT averageRating FROM title_ft_node_b WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
    ) AS combined;

    -- Calculate threshold
    SET @percentile := 0.95;
    SELECT COUNT(*) INTO @totalCount
    FROM (
        SELECT numVotes FROM title_ft WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
        UNION ALL
        SELECT numVotes FROM title_ft_node_b WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
    ) AS combined;
    
    SET @rank_position := CEIL(@percentile * @totalCount);
    SET @rank_position := GREATEST(@rank_position, 1);
    
    SELECT numVotes INTO min_votes_threshold
    FROM (
        SELECT numVotes, ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
        FROM (
            SELECT numVotes FROM title_ft WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
            UNION ALL
            SELECT numVotes FROM title_ft_node_b WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
        ) AS all_votes
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

    -- PHASE 1: Update in appropriate node (guaranteed)
    IF current_startYear IS NOT NULL AND current_startYear >= 2025 THEN
        -- Update in Node A (local)
        START TRANSACTION;
        UPDATE title_ft
        SET numVotes = updated_numVotes,
            averageRating = updated_averageRating,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
        
        INSERT INTO transaction_log (transaction_id, log_sequence, log_type, operation_type, record_id, new_value, timestamp, source_node)
        VALUES (UUID(), 1, 'COMMIT', 'UPDATE', new_tconst, 
                JSON_OBJECT('tconst', new_tconst, 'numVotes', updated_numVotes, 'averageRating', updated_averageRating, 'weightedRating', updated_weightedRating),
                NOW(6), 'NODE_A');
        COMMIT;
    ELSE
        -- Update in Node B (federated)
        START TRANSACTION;
        UPDATE title_ft_node_b
        SET numVotes = updated_numVotes,
            averageRating = updated_averageRating,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
        
        INSERT INTO transaction_log (transaction_id, log_sequence, log_type, operation_type, record_id, new_value, timestamp, source_node)
        VALUES (UUID(), 1, 'COMMIT', 'UPDATE', new_tconst, 
                JSON_OBJECT('tconst', new_tconst, 'numVotes', updated_numVotes, 'averageRating', updated_averageRating, 'weightedRating', updated_weightedRating),
                NOW(6), 'NODE_A');
        COMMIT;
    END IF;

    -- PHASE 2: Attempt replication to Main (best effort)
    SET federated_error = 0;
    START TRANSACTION;
    BEGIN
        DECLARE EXIT HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006, 1296, 1430
        BEGIN
            SET federated_error = 1;
            ROLLBACK;
        END;
        
        UPDATE title_ft_main
        SET numVotes = updated_numVotes,
            averageRating = updated_averageRating,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
        COMMIT;
    END;
    
    IF federated_error = 0 THEN
        SELECT CONCAT('✅ Updated in Node A/B and replicated to Main: ', new_tconst) AS result;
    ELSE
        SELECT CONCAT('⚠️ Updated in Node A/B but Main is unreachable (will recover later): ', new_tconst) AS result;
    END IF;
END$$

DELIMITER ;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Show created procedures
SHOW PROCEDURE STATUS WHERE Db = 'stadvdb-mco2-a';
