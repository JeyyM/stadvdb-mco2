USE `stadvdb-mco2-b`;

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
    -- This does already update the node
    CALL `stadvdb-mco2`.distributed_insert(
        new_tconst,
        new_primaryTitle,
        new_runtimeMinutes,
        new_averageRating,
        new_numVotes,
        new_startYear
    );
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
    CALL `stadvdb-mco2`.distributed_update(
        new_tconst,
        new_primaryTitle,
        new_runtimeMinutes,
        new_averageRating,
        new_numVotes,
        new_startYear
    );
END$$

CREATE PROCEDURE distributed_delete(
    IN new_tconst VARCHAR(12)
)
BEGIN
    CALL `stadvdb-mco2`.distributed_delete(new_tconst);
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
