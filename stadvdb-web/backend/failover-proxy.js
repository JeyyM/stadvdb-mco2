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
const MAIN_API_URL = 'https://stadvdb-mco2-main.onrender.com';

// Track database health
let isDatabaseHealthy = true;
let lastHealthCheck = Date.now();
const HEALTH_CHECK_INTERVAL = 30000; // Check every 30 seconds

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
 * Periodic health check
 */
setInterval(async () => {
  await checkDatabaseHealth();
}, HEALTH_CHECK_INTERVAL);

/**
 * Forward request to Main node
 */
async function forwardToMain(req, res, next) {
  // Only forward if we're NOT the Main node and database is unhealthy
  if (CURRENT_NODE === 'MAIN') {
    return next(); // Main always uses its own database
  }

  // Check if it's time for a health check
  const now = Date.now();
  if (now - lastHealthCheck > HEALTH_CHECK_INTERVAL) {
    lastHealthCheck = now;
    await checkDatabaseHealth();
  }

  // If database is healthy, proceed normally
  if (isDatabaseHealthy) {
    return next();
  }

  // Database is down - proxy to Main
  console.log(`🔄 ${CURRENT_NODE} database unavailable, proxying ${req.method} ${req.path} to Main`);

  try {
    const mainUrl = `${MAIN_API_URL}${req.path}`;
    
    let response;
    if (req.method === 'GET') {
      response = await axios.get(mainUrl, {
        params: req.query,
        timeout: 30000
      });
    } else if (req.method === 'POST') {
      response = await axios.post(mainUrl, req.body, {
        timeout: 30000
      });
    } else if (req.method === 'PUT') {
      response = await axios.put(mainUrl, req.body, {
        timeout: 30000
      });
    } else if (req.method === 'DELETE') {
      response = await axios.delete(mainUrl, {
        data: req.body,
        timeout: 30000
      });
    } else {
      return res.status(405).json({ 
        success: false, 
        message: 'Method not supported for proxy' 
      });
    }

    // Forward the response from Main
    res.status(response.status).json(response.data);
    
  } catch (error) {
    console.error('❌ Error proxying to Main:', error.message);
    
    // If Main is also down, return error
    res.status(503).json({
      success: false,
      message: 'Service temporarily unavailable - both local database and Main node are unreachable',
      error: error.message,
      node: CURRENT_NODE
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
    console.log(`❌ Database connection error detected on ${CURRENT_NODE}, marking as unhealthy`);
    isDatabaseHealthy = false;
    
    // Retry the request by proxying to Main
    return forwardToMain(req, res, () => {
      // If forwarding also fails, pass to next error handler
      next(error);
    });
  }

  // Not a connection error or we're Main - pass to next handler
  next(error);
}

module.exports = {
  forwardToMain,
  handleDatabaseError,
  checkDatabaseHealth,
  isDatabaseHealthy: () => isDatabaseHealthy
};
