-- MAIN NODE SETUP - Federated Access to Slave Nodes

USE `stadvdb-mco2`;

-- Note: title_ft table already exists with complete database (only years 2024-2025)
-- This script adds federated access to slave node fragments
-- Using INTERNAL IP addresses for inter-VM communication

-- Create federated table to access Node A's fragment
-- Initial data: startYear = 2025, but modifiers allow >= 2025
-- Node A is on VM 60052 with internal IP 10.2.14.52
DROP TABLE IF EXISTS `title_ft_node_a`;

CREATE TABLE `title_ft_node_a` (
    tconst VARCHAR(12) PRIMARY KEY,
    primaryTitle VARCHAR(1024),
    runtimeMinutes SMALLINT UNSIGNED,
    averageRating DECIMAL(3,1),
    numVotes INT UNSIGNED,
    weightedRating DECIMAL(10,2),
    startYear SMALLINT UNSIGNED
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.52:3306/stadvdb-mco2-a/title_ft';

-- Create federated table to access Node B's fragment
-- Initial data: startYear = 2024, but modifiers allow < 2025 (including NULL)
-- Node B is on VM 60053 with internal IP 10.2.14.53
DROP TABLE IF EXISTS `title_ft_node_b`;

CREATE TABLE `title_ft_node_b` (
    tconst VARCHAR(12) PRIMARY KEY,
    primaryTitle VARCHAR(1024),
    runtimeMinutes SMALLINT UNSIGNED,
    averageRating DECIMAL(3,1),
    numVotes INT UNSIGNED,
    weightedRating DECIMAL(10,2),
    startYear SMALLINT UNSIGNED
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.53:3306/stadvdb-mco2-b/title_ft';

-- Verify setup
SELECT 'Main Node Setup Complete' AS status;
SELECT COUNT(*) as main_rows FROM `title_ft`;
SELECT COUNT(*) as node_a_rows FROM `title_ft_node_a`;
SELECT COUNT(*) as node_b_rows FROM `title_ft_node_b`;
