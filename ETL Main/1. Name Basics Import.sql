-- Infile needs permissions
-- Open the MySQL terminal in MySQL Command Line Client
-- Then do
-- SHOW VARIABLES LIKE 'local_infile';
-- SET GLOBAL local_infile = 1;

-- SET PERSIST local_infile = 1;   -- persists across restarts
-- SHOW VARIABLES LIKE 'local_infile'; -- should show ON

-- in Home, edit connections
-- in what i named IMDB, go to advanced, put this in the text area with what looks like start commands
	-- OPT_LOCAL_INFILE=1

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS `stadvdb-mco2` DEFAULT CHARACTER SET utf8mb4;

-- Use the schema
USE `stadvdb-mco2`;

DROP TABLE IF EXISTS name_basics;

CREATE TABLE name_basics (
  nconst VARCHAR(12) NOT NULL PRIMARY KEY,
  primaryName VARCHAR(255) NOT NULL,
  birthYear SMALLINT UNSIGNED NULL,
  deathYear SMALLINT UNSIGNED NULL,
  primaryProfession TEXT NULL,
  knownForTitles TEXT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOAD DATA LOCAL INFILE 'C:\\Users\\asus\\Desktop\\STADVDB NEW\\imdbdata\\name.basics.tsv'
INTO TABLE name_basics
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(nconst, primaryName, @birth, @death, primaryProfession, knownForTitles)
SET
  birthYear = CAST(NULLIF(NULLIF(@birth,'\\N'),'') AS UNSIGNED),
  deathYear = CAST(NULLIF(NULLIF(@death,'\\N'),'') AS UNSIGNED);
