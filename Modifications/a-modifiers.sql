USE `stadvdb-mco2-a`;

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

    -- Calculate global mean from Main
    SELECT AVG(averageRating) INTO global_mean
    FROM title_ft_main
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;

    -- Calculate min_votes_threshold (95th percentile) from Main
    SET @percentile := 0.95;
    SELECT COUNT(*) INTO @totalCount
    FROM title_ft_main
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;
    SET @rank_position := CEIL(@percentile * @totalCount);
    SET @rank_position := GREATEST(@rank_position, 1);
    SELECT numVotes INTO min_votes_threshold
    FROM (
        SELECT numVotes, ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
        FROM title_ft_main
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

    -- Insert into Main via federated table
    INSERT INTO title_ft_main
      (tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, weightedRating, startYear)
    VALUES
      (new_tconst, new_primaryTitle, new_runtimeMinutes,
       new_averageRating, new_numVotes, calculated_weightedRating, new_startYear);

    -- Insert into local node if this record belongs here (= 2025)
    IF new_startYear IS NOT NULL AND new_startYear = 2025 THEN
        INSERT INTO title_ft
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, calculated_weightedRating, new_startYear);
    END IF;
END$$

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

    -- Get initial startYear from Main
    SELECT startYear INTO old_startYear
    FROM title_ft_main
    WHERE tconst = new_tconst;

    -- Calculate global mean from Main
    SELECT AVG(averageRating) INTO global_mean
    FROM title_ft_main
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;

    SET @percentile := 0.95;
    SELECT COUNT(*) INTO @totalCount
    FROM title_ft_main
    WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;
    SET @rank_position := CEIL(@percentile * @totalCount);
    SET @rank_position := GREATEST(@rank_position, 1);
    SELECT numVotes INTO min_votes_threshold
    FROM (
        SELECT numVotes, ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
        FROM title_ft_main
        WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
    ) AS ordered_votes
    WHERE row_num = @rank_position
    LIMIT 1;

    SET updated_weightedRating = ROUND(
        (new_numVotes / (new_numVotes + min_votes_threshold)) * new_averageRating
        + (min_votes_threshold / (new_numVotes + min_votes_threshold)) * global_mean, 2
    );

    -- Update Main via federated table
    UPDATE title_ft_main
    SET primaryTitle = new_primaryTitle,
        runtimeMinutes = new_runtimeMinutes,
        averageRating = new_averageRating,
        numVotes = new_numVotes,
        startYear = new_startYear,
        weightedRating = updated_weightedRating
    WHERE tconst = new_tconst;

    -- Handle local node updates based on startYear changes
    IF (old_startYear IS NULL OR old_startYear != 2025) AND (new_startYear = 2025) THEN
        -- Moving to Node A - insert locally
        INSERT INTO title_ft
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, updated_weightedRating, new_startYear);
    ELSEIF (old_startYear = 2025) AND (new_startYear IS NULL OR new_startYear != 2025) THEN
        -- Moving from Node A - delete locally
        DELETE FROM title_ft WHERE tconst = new_tconst;
    ELSEIF new_startYear IS NOT NULL AND new_startYear = 2025 THEN
        -- Staying in Node A - update locally
        UPDATE title_ft
        SET primaryTitle = new_primaryTitle,
            runtimeMinutes = new_runtimeMinutes,
            averageRating = new_averageRating,
            numVotes = new_numVotes,
            startYear = new_startYear,
            weightedRating = updated_weightedRating
        WHERE tconst = new_tconst;
    END IF;
END$$

CREATE PROCEDURE distributed_delete(
    IN new_tconst VARCHAR(12)
)
BEGIN
    -- Delete from Main via federated table
    DELETE FROM title_ft_main WHERE tconst = new_tconst;
    
    -- Delete from local node if exists
    DELETE FROM title_ft WHERE tconst = new_tconst;
END$$


CREATE PROCEDURE distributed_addReviews(
    IN new_tconst VARCHAR(12),
    IN num_new_reviews INT,
    IN new_rating DECIMAL(3,1)
)
BEGIN
    CALL `stadvdb-mco2`.distributed_addReviews(
        new_tconst,
        num_new_reviews,
        new_rating
    );
END$$

CREATE PROCEDURE distributed_select(
    IN select_column VARCHAR(50),
    IN order_direction VARCHAR(4),
    IN limit_count INT UNSIGNED
)
BEGIN
    -- Query from Main which has all data
    SET @sql = CONCAT('
        SELECT * FROM title_ft_main
        ORDER BY ', select_column, ' ', order_direction, '
        LIMIT ', limit_count
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$

CREATE PROCEDURE distributed_search(
    IN search_term VARCHAR(1024),
    IN limit_count INT UNSIGNED
)
BEGIN
    -- Search from Main which has all data
    SELECT * FROM title_ft_main
    WHERE primaryTitle LIKE CONCAT('%', search_term, '%')
    ORDER BY weightedRating DESC
    LIMIT limit_count;
END$$

CREATE PROCEDURE distributed_aggregation()
BEGIN
    -- Get aggregation from Main which has all data
    SELECT 
        COUNT(*) AS movie_count,
        AVG(averageRating) AS average_rating,
        AVG(weightedRating) AS average_weightedRating,
        SUM(numVotes) AS total_votes,
        AVG(numVotes) AS average_votes
    FROM title_ft_main;
END$$

DELIMITER ;
