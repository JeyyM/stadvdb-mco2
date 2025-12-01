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
    INSERT INTO `stadvdb-mco2-a`.title_ft
    (tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear, weightedRating)
    VALUES 
    (new_tconst, new_primaryTitle, new_runtimeMinutes, new_averageRating, new_numVotes, new_startYear, new_averageRating);
    -- the sync to Main will be handled in the Node js Application Layer.
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
    UPDATE title_ft
    SET primaryTitle = new_primaryTitle,
        runtimeMinutes = new_runtimeMinutes,
        averageRating = new_averageRating,
        numVotes = new_numVotes,
        startYear = new_startYear
    WHERE tconst = new_tconst;
END$$

CREATE PROCEDURE distributed_delete(
    IN target_tconst VARCHAR(12)
)
BEGIN
    DELETE FROM title_ft WHERE tconst = target_tconst;
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
    CALL `stadvdb-mco2`.distributed_select(
        select_column,
        order_direction,
        limit_count
    );
END$$

CREATE PROCEDURE distributed_search(
    IN search_term VARCHAR(1024),
    IN limit_count INT UNSIGNED
)
BEGIN
    CALL `stadvdb-mco2`.distributed_search(
        search_term,
        limit_count
    );
END$$

CREATE PROCEDURE distributed_aggregation()
BEGIN
    CALL `stadvdb-mco2`.distributed_aggregation();
END$$

DELIMITER ;
