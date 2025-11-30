-- ============================================================================
-- TEST MYSQL CONNECTIVITY TO NODE A AND NODE B
-- Tests if Main node can connect to the remote MySQL servers
-- ============================================================================

-- ============================================================================
-- METHOD 1: Test with a simple federated table
-- ============================================================================

USE `stadvdb-mco2`;

-- Create a tiny test federated table to Node A
DROP TABLE IF EXISTS test_connection_a;
CREATE TABLE test_connection_a (
    test_id INT
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.52:3306/stadvdb-mco2-a/transaction_log';

-- Try to query it (with timeout)
SET SESSION MAX_EXECUTION_TIME = 5000;
SELECT COUNT(*) as test_result FROM test_connection_a;
SET SESSION MAX_EXECUTION_TIME = 0;

-- If the above worked, connection to Node A is OK!
-- If it timed out or errored, connection failed

-- Create a tiny test federated table to Node B
DROP TABLE IF EXISTS test_connection_b;
CREATE TABLE test_connection_b (
    test_id INT
) ENGINE=FEDERATED
CONNECTION='mysql://g18:fuckingpassword@10.2.14.53:3306/stadvdb-mco2-b/transaction_log';

-- Try to query it (with timeout)
SET SESSION MAX_EXECUTION_TIME = 5000;
SELECT COUNT(*) as test_result FROM test_connection_b;
SET SESSION MAX_EXECUTION_TIME = 0;

-- Clean up test tables
DROP TABLE IF EXISTS test_connection_a;
DROP TABLE IF EXISTS test_connection_b;

-- ============================================================================
-- METHOD 2: Check MySQL error log for federated errors
-- ============================================================================

SHOW VARIABLES LIKE 'log_error';

-- ============================================================================
-- METHOD 3: Check if remote tables actually exist
-- ============================================================================

-- You need to connect to Node A and Node B separately and run:
/*
-- On Node A (10.2.14.52):
USE `stadvdb-mco2-a`;
SHOW TABLES LIKE 'transaction_log';
SELECT COUNT(*) FROM transaction_log;

-- On Node B (10.2.14.53):
USE `stadvdb-mco2-b`;
SHOW TABLES LIKE 'transaction_log';
SELECT COUNT(*) FROM transaction_log;
*/

-- ============================================================================
-- METHOD 4: Check network connectivity from Main's perspective
-- ============================================================================

-- See current connections FROM Main TO other nodes
SELECT 
    ID,
    USER,
    HOST,
    DB,
    COMMAND,
    STATE
FROM information_schema.PROCESSLIST
WHERE HOST LIKE '10.2.14.%';

-- ============================================================================
-- TROUBLESHOOTING GUIDE
-- ============================================================================

/*
If test fails with ERROR 1429 (HY000): Unable to connect to foreign data source

Common causes:
1. MySQL on Node A/B is not running
   - Check: ssh to node and run: sudo systemctl status mysql

2. Firewall blocking port 3306 between VMs
   - Check from Main: telnet 10.2.14.52 3306
   - Check from Main: telnet 10.2.14.53 3306

3. MySQL bind-address is set to 127.0.0.1 (localhost only)
   - Check on Node A/B: SHOW VARIABLES LIKE 'bind_address';
   - Should be '0.0.0.0' or '10.2.14.52' (for Node A)
   - Fix: Edit /etc/mysql/mysql.conf.d/mysqld.cnf
   - Set: bind-address = 0.0.0.0

4. User 'g18' not allowed to connect from Main's IP
   - On Node A/B, check: SELECT user, host FROM mysql.user WHERE user='g18';
   - Should show: g18 | % (or g18 | 10.2.14.51)
   - Fix: GRANT ALL ON *.* TO 'g18'@'10.2.14.51' IDENTIFIED BY 'fuckingpassword';

5. The transaction_log table doesn't exist on Node A/B yet
   - Need to run 2-log-setup-a.sql on Node A
   - Need to run 2-log-setup-b.sql on Node B

6. Port 3306 is not exposed
   - Check: netstat -tlnp | grep 3306

If test succeeds:
- Connection is working!
- Federated tables can communicate
- The issue might be elsewhere (like table structure mismatch)
*/

SELECT 'Connection test complete - check results above' AS status;
