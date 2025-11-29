USE `stadvdb-mco2`;

DROP PROCEDURE IF EXISTS distributed_insert;
DROP PROCEDURE IF EXISTS distributed_update;
DROP PROCEDURE IF EXISTS distributed_delete;
DROP PROCEDURE IF EXISTS distributed_addReviews;
DROP PROCEDURE IF EXISTS distributed_select;
DROP PROCEDURE IF EXISTS distributed_search;
DROP PROCEDURE IF EXISTS distributed_aggregation;

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

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    -- Calculate global mean
    SELECT AVG(averageRating) INTO global_mean
    FROM `stadvdb-mco2`.title_ft
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;

    -- Calculate min_votes_threshold (95th percentile)
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

    -- Calculate weightedRating
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

    -- Use federated tables to insert into remote nodes
    -- >= 2025 = NODE_A, < 2025 (including NULL) = NODE_B
    IF new_startYear IS NOT NULL AND new_startYear >= 2025 THEN
        INSERT INTO title_ft_node_a
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, calculated_weightedRating, new_startYear);
    ELSE
        INSERT INTO title_ft_node_b
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, calculated_weightedRating, new_startYear);
    END IF;

    COMMIT;
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

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    -- Get initial startYear
    SELECT startYear INTO old_startYear
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

    SET updated_weightedRating = ROUND(
        (new_numVotes / (new_numVotes + min_votes_threshold)) * new_averageRating
        + (min_votes_threshold / (new_numVotes + min_votes_threshold)) * global_mean, 2
    );

    UPDATE `stadvdb-mco2`.title_ft
    SET primaryTitle = new_primaryTitle,
        runtimeMinutes = new_runtimeMinutes,
        averageRating = new_averageRating,
        numVotes = new_numVotes,
        startYear = new_startYear,
        weightedRating = updated_weightedRating
    WHERE tconst = new_tconst;

    -- Check if you need to move to a new node (use federated tables)
    -- >= 2025 = NODE_A, < 2025 (including NULL) = NODE_B
    IF (old_startYear IS NULL OR old_startYear < 2025) AND (new_startYear >= 2025) THEN
        -- Moving from B to A
        DELETE FROM title_ft_node_b WHERE tconst = new_tconst;
        INSERT INTO title_ft_node_a
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, updated_weightedRating, new_startYear);
    ELSEIF (old_startYear >= 2025) AND (new_startYear IS NULL OR new_startYear < 2025) THEN
        -- Moving from A to B
        DELETE FROM title_ft_node_a WHERE tconst = new_tconst;
        INSERT INTO title_ft_node_b
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, updated_weightedRating, new_startYear);
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
        ELSE
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

    COMMIT;
END$$

DELIMITER ;

DELIMITER $$

CREATE PROCEDURE distributed_delete(
    IN new_tconst VARCHAR(12)
)

BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    DELETE FROM `stadvdb-mco2`.title_ft
    WHERE tconst = new_tconst;

    -- Use federated tables to delete from remote nodes
    DELETE FROM title_ft_node_a WHERE tconst = new_tconst;
    DELETE FROM title_ft_node_b WHERE tconst = new_tconst;

    COMMIT;
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
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION

    IF num_new_reviews < 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'num_new_reviews cannot be negative';
    END IF;

    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

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

    -- Use federated tables to update remote nodes
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

    COMMIT;
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
