-- Use the schema
USE `stadvdb-mco2`;

DROP TABLE IF EXISTS title_ft;

CREATE TABLE title_ft (
  tconst VARCHAR(12) NOT NULL PRIMARY KEY,
  primaryTitle VARCHAR(1024) NOT NULL,
  runtimeMinutes SMALLINT UNSIGNED NULL,
  averageRating DECIMAL(3,1) NULL,
  numVotes INT UNSIGNED NULL,
  startYear SMALLINT UNSIGNED NULL,
  weightedRating DECIMAL(4,2) NULL
) ENGINE=InnoDB;

-- Using SET to create variables since there seems to be some
-- issues when putting it in a subquery. It just wouldn't let me.

-- Global mean rating
SET @totalMean := (
  SELECT AVG(averageRating)
  FROM title_ratings
  WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
);

-- Count, used for the percentile
SELECT COUNT(*) INTO @totalCount
FROM title_ratings
WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL;

-- Percentile position
-- The := is for assigning into variables made
SET @percentile := 0.95;
SET @rank_position := CEIL(@percentile * @totalCount); -- To get the offset index to get to the 95th item
SET @rank_position := GREATEST(@rank_position, 1);

-- min_votes_threshold = numVotes at that rank using ROW_NUMBER()
-- Needed for the formula
SELECT numVotes INTO @min_votes_threshold
FROM (
  SELECT
    numVotes,
    ROW_NUMBER() OVER (ORDER BY numVotes) AS row_num
  FROM title_ratings
  WHERE averageRating IS NOT NULL AND numVotes IS NOT NULL
) AS ordered_votes
WHERE row_num = @rank_position
LIMIT 1;

-- Weighted Rating Formula
-- weighted_rating = (v / (v + m)) * R + (m / (v + m)) * C
-- v = number of votes for the title (r.numVotes)
-- m = minimum votes threshold (95th percentile of vote counts, @min_votes_threshold)
-- R = average rating for the title (r.averageRating)
-- C = global mean rating across all titles (@totalMean)

INSERT INTO title_ft
  (tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear, weightedRating)
SELECT
  b.tconst,
  b.primaryTitle,
  b.runtimeMinutes,
  r.averageRating,
  r.numVotes,
  b.startYear,
  CASE
    WHEN r.numVotes IS NOT NULL AND r.averageRating IS NOT NULL AND @min_votes_threshold > 0 THEN
      ROUND(
        (r.numVotes / (r.numVotes + @min_votes_threshold)) * r.averageRating
        + (@min_votes_threshold / (r.numVotes + @min_votes_threshold)) * @totalMean
      , 2)
    ELSE NULL
  END AS weightedRating
FROM title_basics AS b
LEFT JOIN title_ratings AS r
  ON r.tconst = b.tconst
WHERE b.titleType IS NOT NULL;

SELECT * FROM title_ft LIMIT 10;
