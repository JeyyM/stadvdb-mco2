-- This script reloads the updated stored procedures into Main database
-- Run this after any changes to 4-main-modifiers.sql

SOURCE ./Modifications/4-main-modifiers.sql;

-- Verify procedures loaded
SELECT ROUTINE_NAME, ROUTINE_TYPE, CREATED 
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = 'stadvdb-mco2' 
AND ROUTINE_NAME IN ('distributed_insert', 'distributed_update', 'distributed_delete', 'log_to_remote_node')
ORDER BY CREATED DESC;
