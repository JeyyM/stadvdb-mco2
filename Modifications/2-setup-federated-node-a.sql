-- ============================================================================
-- SETUP FEDERATED TABLES FOR NODE A
-- This creates federated table pointing to Node B so Node A can act as
-- secondary coordinator and aggregate data from both nodes
-- ============================================================================

USE `stadvdb-mco2-a`;

-- Drop existing federated table if it exists
DROP TABLE IF EXISTS title_ft_node_b;

-- Create federated table pointing to Node B
CREATE TABLE IF NOT EXISTS title_ft_node_b (
    tconst VARCHAR(12) NOT NULL,
    primaryTitle VARCHAR(1024),
    runtimeMinutes SMALLINT UNSIGNED,
    averageRating DECIMAL(3,1),
    numVotes INT UNSIGNED,
    weightedRating DECIMAL(10,2),
    startYear SMALLINT UNSIGNED,
    PRIMARY KEY (tconst)
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.53:3306/stadvdb-mco2-b/title_ft';

-- Verify the federated table
SELECT 'Federated table title_ft_node_b created successfully' AS status;
SELECT COUNT(*) AS node_b_record_count FROM title_ft_node_b;

-- Show table structure
DESCRIBE title_ft_node_b;
