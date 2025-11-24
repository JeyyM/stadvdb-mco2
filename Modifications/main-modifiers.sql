USE `stadvdb-mco2`;

DROP PROCEDURE IF EXISTS main_insert;

DELIMITER //

CREATE PROCEDURE main_insert (
    IN new_tconst VARCHAR(12),
    IN new_primaryTitle VARCHAR(1024),
    IN new_runtimeMinutes SMALLINT UNSIGNED,
    IN new_averageRating DECIMAL(3,1),
    IN new_numVotes INT UNSIGNED,
    IN new_startYear SMALLINT UNSIGNED,
    IN new_weightedRating DECIMAL(4,2)
)

BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    INSERT INTO `stadvdb-mco2`.title_ft
      (tconst, primaryTitle, runtimeMinutes,
       averageRating, numVotes, startYear, weightedRating)
    VALUES
      (new_tconst, new_primaryTitle, new_runtimeMinutes,
       new_averageRating, new_numVotes, new_startYear, new_weightedRating);

    IF new_startYear IS NULL OR new_startYear < 2010 THEN
        INSERT INTO `stadvdb-mco2-b`.title_ft
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, new_startYear, new_weightedRating);
    ELSE
        INSERT INTO `stadvdb-mco2-a`.title_ft
        VALUES (new_tconst, new_primaryTitle, new_runtimeMinutes,
                new_averageRating, new_numVotes, new_startYear, new_weightedRating);
    END IF;

    COMMIT;
END//

DELIMITER ;
