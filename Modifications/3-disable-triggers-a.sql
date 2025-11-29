-- ============================================================================
-- DISABLE TRIGGERS ON NODE A
-- Since Main node logs to Node A's transaction_log via federated tables,
-- we don't need triggers on Node A anymore (they would create duplicates)
-- ============================================================================

USE `stadvdb-mco2-a`;

-- Drop all triggers
DROP TRIGGER IF EXISTS title_ft_before_insert;
DROP TRIGGER IF EXISTS title_ft_after_insert;
DROP TRIGGER IF EXISTS title_ft_before_update;
DROP TRIGGER IF EXISTS title_ft_after_update;
DROP TRIGGER IF EXISTS title_ft_before_delete;
DROP TRIGGER IF EXISTS title_ft_after_delete;

SELECT 'Node A triggers disabled - Main node now logs to Node A via federated tables' AS status;
