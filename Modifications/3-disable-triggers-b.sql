-- DISABLE TRIGGERS ON NODE B

USE `stadvdb-mco2-b`;

-- Drop all triggers
DROP TRIGGER IF EXISTS title_ft_before_insert;
DROP TRIGGER IF EXISTS title_ft_after_insert;
DROP TRIGGER IF EXISTS title_ft_before_update;
DROP TRIGGER IF EXISTS title_ft_after_update;
DROP TRIGGER IF EXISTS title_ft_before_delete;
DROP TRIGGER IF EXISTS title_ft_after_delete;

SELECT 'Node B triggers disabled - Main node now logs to Node B via federated tables' AS status;
