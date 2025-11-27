-- Use the schema
USE `stadvdb-mco2-a`;

DROP TABLE IF EXISTS title_basics;

CREATE TABLE title_basics (
  tconst VARCHAR(12) NOT NULL PRIMARY KEY,
  titleType VARCHAR(32) NOT NULL,
  primaryTitle VARCHAR(1024) NOT NULL,
  originalTitle VARCHAR(1024) NOT NULL,
  isAdult TINYINT(1) NOT NULL,
  startYear SMALLINT UNSIGNED NULL,
  endYear SMALLINT UNSIGNED NULL,
  runtimeMinutes INT NULL,
  genres TEXT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOAD DATA LOCAL INFILE 'C:\\Users\\zan_laptop\\Desktop\\stadvdb-mco2-stuff\\imdbdata\\title.basics.tsv'
INTO TABLE title_basics
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@tconst, @titleType, @primaryTitle, @originalTitle, @isAdult, @startYear, @endYear, @runtime, @genres)
SET
  tconst = NULLIF(@tconst,'\\N'),
  titleType = NULLIF(@titleType,'\\N'),
  primaryTitle = NULLIF(@primaryTitle,'\\N'),
  originalTitle = NULLIF(@originalTitle,'\\N'),
  isAdult = CAST(NULLIF(@isAdult,'\\N') AS UNSIGNED),
  startYear = CAST(NULLIF(@startYear,'\\N') AS UNSIGNED),
  endYear = CAST(NULLIF(@endYear,'\\N') AS UNSIGNED),
  runtimeMinutes = CAST(NULLIF(@runtime,'\\N') AS UNSIGNED),
  genres = NULLIF(@genres,'\\N');
