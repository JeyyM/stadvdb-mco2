-- Use the schema
USE `stadvdb-mco2-b`;

DROP TABLE IF EXISTS title_ratings;

CREATE TABLE title_ratings (
  tconst VARCHAR(12) NOT NULL PRIMARY KEY,
  averageRating DECIMAL(3,1) NOT NULL,
  numVotes INT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOAD DATA LOCAL INFILE 'C:\\Users\\asus\\Desktop\\STADVDB NEW\\imdbdata\\title.ratings.tsv'
INTO TABLE title_ratings
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@tconst, @rating, @votes)
SET
  tconst = NULLIF(@tconst,'\\N'),
  averageRating = CAST(NULLIF(@rating,'\\N') AS DECIMAL(3,1)),
  numVotes = CAST(NULLIF(@votes,'\\N') AS UNSIGNED);
