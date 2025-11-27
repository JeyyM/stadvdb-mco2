const db = require('./db');
const { v4: uuidv4 } = require('uuid');

class RecoveryManager {
  
  // Write-Ahead Logging
  async logTransaction(type, targetNode, sql, params) {
    const transactionId = uuidv4();
    const query = `
      INSERT INTO transaction_logs 
      (transaction_uuid, query_type, target_node, query_sql, query_params, status)
      VALUES (?, ?, ?, ?, ?, 'PENDING')
    `;
    
    try {
      await db.query(query, [
        transactionId, 
        type, 
        targetNode, 
        sql, 
        JSON.stringify(params)
      ]);
      return transactionId;
    } catch (error) {
      console.error("CRITICAL: Failed to write to transaction log!", error);
      return null;
    }
  }

  // ts an update status: COMMITTED if success, FAILED if crashed
  async updateLogStatus(transactionId, status, errorMessage = null) {
    const query = `
      UPDATE transaction_logs 
      SET status = ?, error_message = ?
      WHERE transaction_uuid = ?
    `;
    try {
      await db.query(query, [status, errorMessage, transactionId]);
    } catch (error) {
      console.error("Error updating transaction log status:", error);
    }
  }

  // Run this when a node comes back online
  async recoverFailedTransactions(targetNode) {
    console.log(`Checking for failed transactions for ${targetNode}...`);
    
    // Find all transactions that failed or are stuck in pneding for this node
    const fetchQuery = `
      SELECT * FROM transaction_logs 
      WHERE target_node = ? AND status IN ('FAILED', 'PENDING')
      ORDER BY created_at ASC
    `;

    try {
      const [logs] = await db.query(fetchQuery, [targetNode]);
      
      if (logs.length === 0) {
        console.log(`No failed transactions found for ${targetNode}.`);
        return;
      }

      console.log(`Found ${logs.length} failed transactions. Attempting recovery...`);

      for (const log of logs) {
        try {
          const params = JSON.parse(log.query_params);
          
          console.log(`Replaying transaction ${log.transaction_uuid}...`);
          await db.query(log.query_sql, params);

          await this.updateLogStatus(log.transaction_uuid, 'COMMITTED');
          console.log(`Transaction ${log.transaction_uuid} recovered successfully.`);

        } catch (retryError) {
          console.error(`Recovery failed for ${log.transaction_uuid}:`, retryError.message);
        }
      }
    } catch (error) {
      console.error("Error during recovery process:", error);
    }
  }
}

module.exports = new RecoveryManager();