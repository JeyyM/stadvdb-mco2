-- NODE B SETUP - Federated Access to Main (Master)

USE `stadvdb-mco2-b`;

-- Note: title_ft table already created via ETL B/4. create title_ft.sql
-- This script only adds federated access to Main's complete database

-- Create federated table to access Main's complete title_ft
DROP TABLE IF EXISTS `title_ft_main`;

CREATE TABLE `title_ft_main` (
    tconst VARCHAR(12) PRIMARY KEY,
    primaryTitle VARCHAR(1024),
    runtimeMinutes SMALLINT UNSIGNED,
    averageRating DECIMAL(3,1),
    numVotes INT UNSIGNED,
    weightedRating DECIMAL(10,2),
    startYear SMALLINT UNSIGNED
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.51:3306/stadvdb-mco2/title_ft';

-- Verify setup
SELECT 'Node B Setup Complete' AS status;
SELECT COUNT(*) as local_rows FROM `title_ft`;
SELECT COUNT(*) as main_rows FROM `title_ft_main`;
