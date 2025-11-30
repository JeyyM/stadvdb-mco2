/**
 * AUTOMATIC RECOVERY MODULE
 * Handles automatic detection and recovery of missing transactions
 * Runs ONLY on server startup with checkpoint system
 */

const db = require('./db');

// Recovery state tracking
let isRecoveryInProgress = false;
let recoveryPromise = null;

/**
 * Check if recovery is currently in progress
 */
function isRecovering() {
  return isRecoveryInProgress;
}

/**
 * Wait for recovery to complete before processing requests
 */
async function waitForRecovery() {
  if (isRecoveryInProgress && recoveryPromise) {
    console.log('Waiting for recovery to complete...');
    await recoveryPromise;
  }
}

/**
 * Check if this is the Main node based on environment variables
 */
function isMainNode() {
  const dbName = process.env.DB_NAME || '';
  return dbName === 'stadvdb-mco2' || dbName.includes('main');
}

/**
 * Check if this is Node A
 */
function isNodeA() {
  const dbName = process.env.DB_NAME || '';
  return dbName === 'stadvdb-mco2-a' || dbName.includes('-a');
}

/**
 * Check if this is Node B
 */
function isNodeB() {
  const dbName = process.env.DB_NAME || '';
  return dbName === 'stadvdb-mco2-b' || dbName.includes('-b');
}

/**
 * Run recovery for Node A (from Main's logs)
 * Only runs if this IS the Main node
 */
async function recoverNodeA(sinceTimestamp = null) {
  if (!isMainNode()) {
    console.log('Skipping Node A recovery - not Main node');
    return { skipped: true };
  }

  try {
    const since = sinceTimestamp || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 19).replace('T', ' ');
    
    console.log(`Checking for missing transactions on Node A since ${since}...`);
    
    // Find missing transactions
    const [missing] = await db.query('CALL find_missing_on_node_a(?)', [since]);
    
    if (!missing || !missing[0] || missing[0].length === 0) {
      console.log('Node A is synchronized - no missing transactions');
      return { recovered: 0, skipped: false };
    }
    
    console.log(`Found ${missing[0].length} missing transactions on Node A`);
    
    // Run full recovery
    console.log('Starting automatic recovery for Node A...');
    await db.query('CALL full_recovery_node_a(?)', [since]);
    
    console.log(`Node A recovery complete - ${missing[0].length} transactions replayed`);
    return { recovered: missing[0].length, skipped: false };
    
  } catch (error) {
    console.error('Error during Node A recovery:', error.message);
    return { error: error.message, skipped: false };
  }
}

/**
 * Run recovery for Node B (from Main's logs)
 * Only runs if this IS the Main node
 */
async function recoverNodeB(sinceTimestamp = null) {
  if (!isMainNode()) {
    console.log('Skipping Node B recovery - not Main node');
    return { skipped: true };
  }

  try {
    const since = sinceTimestamp || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 19).replace('T', ' ');
    
    console.log(`Checking for missing transactions on Node B since ${since}...`);
    
    const [missing] = await db.query('CALL find_missing_on_node_b(?)', [since]);
    
    if (!missing || !missing[0] || missing[0].length === 0) {
      console.log('Node B is synchronized - no missing transactions');
      return { recovered: 0, skipped: false };
    }
    
    console.log(`Found ${missing[0].length} missing transactions on Node B`);
    
    console.log('Starting automatic recovery for Node B...');
    await db.query('CALL full_recovery_node_b(?)', [since]);
    
    console.log(`Node B recovery complete - ${missing[0].length} transactions replayed`);
    return { recovered: missing[0].length, skipped: false };
    
  } catch (error) {
    console.error('Error during Node B recovery:', error.message);
    return { error: error.message, skipped: false };
  }
}

/**
 * Run recovery for Main node (from Node A/B logs)
 * Only runs if this IS the Main node AND Main was down
 */
async function recoverMainNode(sinceTimestamp = null) {
  if (!isMainNode()) {
    console.log('Skipping Main recovery - not Main node');
    return { skipped: true };
  }

  console.log('Main node recovery not yet implemented (Case #2)');
  return { skipped: true, notImplemented: true };
}

/**
 * Check for uncommitted transactions (potential failures)
 */
async function checkUncommittedTransactions() {
  try {
    console.log('🔍 Checking for uncommitted transactions...');
    const [result] = await db.query('CALL check_uncommitted_transactions()');
    
    if (result && result[0] && result[0].length > 0) {
      console.warn(`Found ${result[0].length} uncommitted transactions:`);
      console.table(result[0]);
      return { count: result[0].length, transactions: result[0] };
    } else {
      console.log('No uncommitted transactions found');
      return { count: 0, transactions: [] };
    }
  } catch (error) {
    console.error('Error checking uncommitted transactions:', error.message);
    return { error: error.message };
  }
}

/**
 * Run full recovery check on startup
 * This is called when the server starts
 * BLOCKS all incoming requests until complete
 */
async function runStartupRecovery() {
  isRecoveryInProgress = true;

  recoveryPromise = (async () => {
    try {
      console.log('\n==========================================================');
      console.log('STARTING AUTOMATIC RECOVERY CHECK');
      console.log('ALL REQUESTS BLOCKED UNTIL RECOVERY COMPLETES');
      console.log('==========================================================\n');
      
      const nodeName = process.env.DB_NAME || 'unknown';
      console.log(`Current Node: ${nodeName}`);
      console.log(`Recovery Mode: ${isMainNode() ? 'MAIN (can recover Node A/B)' : 'WORKER (recovery disabled)'}\n`);

      await checkUncommittedTransactions();
      
      // Only Main node can perform recovery
      if (isMainNode()) {
        console.log('\n--- Recovering Node A ---');
        await recoverNodeA();
        
        console.log('\n--- Recovering Node B ---');
        await recoverNodeB();
        
        console.log('\n--- Recovering Main (not implemented) ---');
        await recoverMainNode();
      } else {
        console.log('\nThis is not the Main node - automatic recovery disabled');
        console.log('Only Main node can perform automatic recovery for Node A/B');
      }
      
      console.log('\n==========================================================');
      console.log('STARTUP RECOVERY CHECK COMPLETE');
      console.log('SERVER NOW ACCEPTING REQUESTS');
      console.log('==========================================================\n');
    } finally {
      isRecoveryInProgress = false;
      recoveryPromise = null;
    }
  })();
  
  return recoveryPromise;
}

/**
 * Run periodic recovery check
 * This runs every X minutes while the server is running
 */
async function runPeriodicRecovery() {
  console.log('\n--- Periodic Recovery Check (Background) ---');
  
  if (!isMainNode()) {
    return;
  }
  
  // Only check last 15 minutes to avoid performance issues
  const since = new Date(Date.now() - 15 * 60 * 1000).toISOString().slice(0, 19).replace('T', ' ');
  
  await recoverNodeA(since);
  await recoverNodeB(since);
  
  console.log('--- Periodic Check Complete ---\n');
}

/**
 * Start periodic recovery checks
 * Runs every 5 minutes by default
 */
function startPeriodicRecovery(intervalMinutes = 5) {
  if (!isMainNode()) {
    console.log('Periodic recovery disabled - not Main node');
    return null;
  }
  
  console.log(`Starting periodic recovery checks every ${intervalMinutes} minutes`);
  
  const intervalMs = intervalMinutes * 60 * 1000;
  const interval = setInterval(runPeriodicRecovery, intervalMs);
  
  return interval;
}

module.exports = {
  isMainNode,
  isNodeA,
  isNodeB,
  recoverNodeA,
  recoverNodeB,
  recoverMainNode,
  checkUncommittedTransactions,
  runStartupRecovery,
  isRecovering,
  waitForRecovery
};
