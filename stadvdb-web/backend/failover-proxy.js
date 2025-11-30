/**
 * Failover Proxy Middleware
 * 
 * When Node A or Node B database is unavailable, this middleware automatically
 * proxies requests to the Main node instead of failing completely.
 * 
 * This allows the system to continue operating even when worker nodes are offline.
 */

const axios = require('axios');
const db = require('./db');

// Hardcoded configuration - detect node type from DB_NAME
const DB_NAME = process.env.DB_NAME || 'stadvdb-mco2';
const CURRENT_NODE = DB_NAME === 'stadvdb-mco2' ? 'MAIN' : 
                     DB_NAME === 'stadvdb-mco2-a' ? 'NODE_A' : 
                     DB_NAME === 'stadvdb-mco2-b' ? 'NODE_B' : 'MAIN';

// Failover hierarchy: Main → Node A → Node B
const MAIN_API_URL = 'https://stadvdb-mco2-main.onrender.com';
const NODE_A_API_URL = 'https://stadvdb-mco2-a.onrender.com';
const NODE_B_API_URL = 'https://stadvdb-mco2-b.onrender.com';

// Track health of all nodes
let isDatabaseHealthy = true;
let lastHealthCheck = Date.now();
const HEALTH_CHECK_INTERVAL = 30000; // Check every 30 seconds

// Cache for remote node health
let mainNodeHealthy = true;
let nodeAHealthy = true;
let nodeBHealthy = true;
let lastRemoteHealthCheck = 0;
const REMOTE_HEALTH_CHECK_INTERVAL = 15000; // Check remote nodes every 15 seconds

/**
 * Check if database connection is healthy
 */
async function checkDatabaseHealth() {
  try {
    await db.query('SELECT 1');
    isDatabaseHealthy = true;
    return true;
  } catch (error) {
    console.error('❌ Database health check failed:', error.message);
    isDatabaseHealthy = false;
    return false;
  }
}

/**
 * Check health of remote nodes
 */
async function checkRemoteNodeHealth() {
  const now = Date.now();
  if (now - lastRemoteHealthCheck < REMOTE_HEALTH_CHECK_INTERVAL) {
    return; // Don't check too frequently
  }
  lastRemoteHealthCheck = now;

  // Check Main node health
  if (CURRENT_NODE !== 'MAIN') {
    try {
      await axios.get(`${MAIN_API_URL}/api/recovery/status`, { timeout: 5000 });
      mainNodeHealthy = true;
    } catch (error) {
      mainNodeHealthy = false;
      console.log('⚠️ Main node appears to be down');
    }
  }

  // Check Node A health (if we're Node B)
  if (CURRENT_NODE === 'NODE_B') {
    try {
      await axios.get(`${NODE_A_API_URL}/api/recovery/status`, { timeout: 5000 });
      nodeAHealthy = true;
    } catch (error) {
      nodeAHealthy = false;
      console.log('⚠️ Node A appears to be down');
    }
  }
}

/**
 * Determine which node to proxy to based on failover hierarchy
 * Hierarchy: Main → Node A → Node B (local fallback)
 */
async function getProxyTarget() {
  // For Main node, check DB health immediately to detect failures
  if (CURRENT_NODE === 'MAIN') {
    const dbHealthy = await checkDatabaseHealth();
    if (!dbHealthy && nodeAHealthy) {
      return { url: NODE_A_API_URL, name: 'Node A (backup coordinator)' };
    }
    return null; // Either Main's DB is healthy (use local), or no backup available
  }
  
  // Node B: Try Main first, then Node A
  if (CURRENT_NODE === 'NODE_B') {
    if (mainNodeHealthy) {
      return { url: MAIN_API_URL, name: 'Main' };
    } else if (nodeAHealthy) {
      return { url: NODE_A_API_URL, name: 'Node A' };
    }
    return null; // No proxy available, use local data
  } 
  
  // Node A: Try Main only (A is second in hierarchy)
  if (CURRENT_NODE === 'NODE_A') {
    if (mainNodeHealthy) {
      return { url: MAIN_API_URL, name: 'Main' };
    }
    return null; // No proxy available, Node A will handle distributed operations
  }
  
  return null;
}

/**
 * Periodic health check
 */
setInterval(async () => {
  await checkDatabaseHealth();
  await checkRemoteNodeHealth();
}, HEALTH_CHECK_INTERVAL);

/**
 * Forward request to another node based on failover hierarchy
 */
async function forwardToMain(req, res, next) {
  // Check remote node health first
  await checkRemoteNodeHealth();

  // Determine proxy target
  const proxyTarget = getProxyTarget();
  
  if (!proxyTarget) {
    // No proxy target available
    if (CURRENT_NODE === 'MAIN') {
      // Main uses its own database
      return next();
    } else if (CURRENT_NODE === 'NODE_A') {
      // Node A acts as backup coordinator when Main is down
      return next();
    } else {
      // Node B has no fallback - return error
      return res.status(503).json({
        success: false,
        message: 'Service temporarily unavailable - all coordinator nodes unreachable',
        node: CURRENT_NODE
      });
    }
  }

  // Proxy to target node (Main or Node A for Node B)
  console.log(`🔄 ${CURRENT_NODE} proxying ${req.method} ${req.originalUrl} to ${proxyTarget.name}`);

  try {
    // Use originalUrl to get the full path with query string
    const targetUrl = `${proxyTarget.url}${req.originalUrl}`;
    
    console.log(`   Proxying to: ${targetUrl}`);
    
    let response;
    if (req.method === 'GET') {
      response = await axios.get(targetUrl, {
        timeout: 30000
      });
    } else if (req.method === 'POST') {
      response = await axios.post(targetUrl, req.body, {
        headers: {
          'Content-Type': 'application/json'
        },
        timeout: 30000
      });
    } else if (req.method === 'PUT') {
      response = await axios.put(targetUrl, req.body, {
        headers: {
          'Content-Type': 'application/json'
        },
        timeout: 30000
      });
    } else if (req.method === 'DELETE') {
      response = await axios.delete(targetUrl, {
        data: req.body,
        headers: {
          'Content-Type': 'application/json'
        },
        timeout: 30000
      });
    } else {
      return res.status(405).json({ 
        success: false, 
        message: 'Method not supported for proxy' 
      });
    }

    // Forward the response from target
    console.log(`   ✅ Proxy success: ${response.status} from ${proxyTarget.name}`);
    res.status(response.status).json(response.data);
    
  } catch (error) {
    console.error(`❌ Error proxying to ${proxyTarget.name}:`, error.message);
    
    // Mark target as unhealthy
    if (proxyTarget.url === MAIN_API_URL) {
      mainNodeHealthy = false;
    } else if (proxyTarget.url === NODE_A_API_URL) {
      nodeAHealthy = false;
    }
    
    // Try next in failover chain
    if (CURRENT_NODE === 'NODE_B' && proxyTarget.url === MAIN_API_URL && nodeAHealthy) {
      console.log('🔄 Retrying with Node A...');
      const retryTarget = { url: NODE_A_API_URL, name: 'Node A' };
      try {
        const targetUrl = `${retryTarget.url}${req.originalUrl}`;
        let response;
        if (req.method === 'GET') {
          response = await axios.get(targetUrl, { timeout: 30000 });
        } else if (req.method === 'POST') {
          response = await axios.post(targetUrl, req.body, { headers: { 'Content-Type': 'application/json' }, timeout: 30000 });
        }
        console.log(`   ✅ Retry success: ${response.status} from ${retryTarget.name}`);
        return res.status(response.status).json(response.data);
      } catch (retryError) {
        console.error(`❌ Retry to Node A also failed: ${retryError.message}`);
        nodeAHealthy = false;
      }
    }
    
    // All proxies failed - use local data if available
    if (isDatabaseHealthy) {
      console.log(`⚠️ All remote nodes down, falling back to local ${CURRENT_NODE} data`);
      return next();
    }
    
    // Everything is down
    res.status(503).json({
      success: false,
      message: 'Service temporarily unavailable - all nodes unreachable',
      error: error.message,
      node: CURRENT_NODE,
      attemptedTargets: [proxyTarget.name]
    });
  }
}

/**
 * Error handler that catches database connection errors and retries via Main
 */
function handleDatabaseError(error, req, res, next) {
  // Check if error is a database connection error
  const isConnectionError = 
    error.code === 'ECONNREFUSED' ||
    error.code === 'ETIMEDOUT' ||
    error.code === 'ENOTFOUND' ||
    error.errno === 'ECONNREFUSED' ||
    error.sqlState === 'HY000' ||
    error.message?.includes('connect ETIMEDOUT') ||
    error.message?.includes('connect ECONNREFUSED');

  if (isConnectionError && CURRENT_NODE !== 'MAIN') {
    console.log(`❌ Database connection error detected on ${CURRENT_NODE}, marking as unhealthy and proxying to Main`);
    isDatabaseHealthy = false;
    
    // Proxy this request to Main
    return forwardToMain(req, res, next);
  }

  // Not a connection error or we're Main - pass to next handler
  next(error);
}

/**
 * Execute database query with automatic failover to Main
 * This wraps db.query() and proxies to Main's HTTP API if database is unavailable
 */
async function queryWithFailover(sql, params, req, res) {
  // If we're Main or database is healthy, try local query first
  if (CURRENT_NODE === 'MAIN' || isDatabaseHealthy) {
    try {
      const result = await db.query(sql, params);
      isDatabaseHealthy = true; // Mark as healthy on success
      return { success: true, result };
    } catch (error) {
      // Check if it's a connection error
      const isConnectionError = 
        error.code === 'ECONNREFUSED' ||
        error.code === 'ETIMEDOUT' ||
        error.code === 'ENOTFOUND' ||
        error.errno === 'ECONNREFUSED' ||
        error.sqlState === 'HY000' ||
        error.message?.includes('connect ETIMEDOUT') ||
        error.message?.includes('connect ECONNREFUSED');

      if (isConnectionError && CURRENT_NODE !== 'MAIN') {
        console.log(`❌ Database connection failed on ${CURRENT_NODE}, attempting proxy to Main`);
        isDatabaseHealthy = false;
        // Fall through to proxy logic below
      } else {
        // Not a connection error, or we're Main - throw it
        throw error;
      }
    }
  }

  // Database is unhealthy and we're not Main - proxy to Main's HTTP API
  if (CURRENT_NODE !== 'MAIN' && !isDatabaseHealthy) {
    console.log(`🔄 Proxying request to Main (DB unhealthy on ${CURRENT_NODE})`);
    return forwardToMain(req, res, () => {
      throw new Error('Both local database and Main node are unreachable');
    });
  }
}

module.exports = {
  forwardToMain,
  handleDatabaseError,
  checkDatabaseHealth,
  queryWithFailover,
  getProxyTarget,
  isDatabaseHealthy: () => isDatabaseHealthy,
  isMainHealthy: () => mainNodeHealthy,
  isNodeAHealthy: () => nodeAHealthy,
  isNodeBHealthy: () => nodeBHealthy,
  getCurrentNode: () => CURRENT_NODE
};
