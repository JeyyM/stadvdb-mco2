-- DISTRIBUTED PROCEDURES FOR NODE A (Secondary Coordinator)

USE `stadvdb-mco2-a`;

DROP PROCEDURE IF EXISTS distributed_insert;
DROP PROCEDURE IF EXISTS distributed_update;
DROP PROCEDURE IF EXISTS distributed_delete;
DROP PROCEDURE IF EXISTS distributed_addReviews;
DROP PROCEDURE IF EXISTS distributed_select;
DROP PROCEDURE IF EXISTS distributed_search;
DROP PROCEDURE IF EXISTS distributed_aggregation;

DELIMITER $$

-- DISTRIBUTED AGGREGATION

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

-- DISTRIBUTED SELECT

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

-- DISTRIBUTED SEARCH

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

-- DISTRIBUTED INSERT

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
    DECLARE federated_error INT DEFAULT 0;
    
    DECLARE CONTINUE HANDLER FOR 1429, 1158, 1159, 1189, 2013, 2006
    BEGIN
        SET federated_error = 1;
    END;

    -- Calculate global mean from combined data
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

    IF new_numVotes IS NULL OR new_numVotes = 0 THEN
        SET calculated_weightedRating = global_mean;
    ELSE
        SET calculated_weightedRating = ROUND(
            (new_numVotes / (new_numVotes + min_votes_threshold)) * new_averageRating
            + (min_votes_threshold / (new_numVotes + min_votes_threshold)) * global_mean, 2
        );
    END IF;

    -- Insert to appropriate node based on startYear
    IF new_startYear IS NOT NULL AND new_startYear >= 2025 THEN
        -- Insert to Node A (local)
        INSERT INTO title_ft
        (tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear, weightedRating)
        VALUES
        (new_tconst, new_primaryTitle, new_runtimeMinutes, new_averageRating, new_numVotes, new_startYear, calculated_weightedRating);
    ELSE
        -- Insert to Node B (federated)
        INSERT INTO title_ft_node_b
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes, new_averageRating, new_numVotes, calculated_weightedRating, new_startYear);
    END IF;
END$$

DELIMITER ;

DELIMITER $$

-- DISTRIBUTED UPDATE

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

-- DISTRIBUTED DELETE

CREATE PROCEDURE distributed_delete(
    IN new_tconst VARCHAR(12)
)
BEGIN
    DECLARE old_startYear SMALLINT UNSIGNED;
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

    IF found_in_a THEN
        -- Delete from Node A
        DELETE FROM title_ft WHERE tconst = new_tconst;
    ELSE
        -- Try to delete from Node B
        DELETE FROM title_ft_node_b WHERE tconst = new_tconst;
    END IF;
END$$

DELIMITER ;

DELIMITER $$

-- DISTRIBUTED ADD REVIEWS

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

    -- Update in appropriate node
    IF current_startYear IS NOT NULL AND current_startYear >= 2025 THEN
        -- Update in Node A
        UPDATE title_ft
        SET numVotes = updated_numVotes,
            averageRating = updated_averageRating,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
    ELSE
        -- Update in Node B
        UPDATE title_ft_node_b
        SET numVotes = updated_numVotes,
            averageRating = updated_averageRating,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
    END IF;
END$$

DELIMITER ;

-- VERIFICATION

-- Show created procedures
SHOW PROCEDURE STATUS WHERE Db = 'stadvdb-mco2-a';
