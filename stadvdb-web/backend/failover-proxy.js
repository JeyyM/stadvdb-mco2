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
  // Only forward if we're NOT the Main node
  if (CURRENT_NODE === 'MAIN') {
    return next(); // Main always uses its own database
  }

  // Database is down - proxy to Main
  console.log(`🔄 ${CURRENT_NODE} database unavailable, proxying ${req.method} ${req.originalUrl} to Main`);

  try {
    // Use originalUrl to get the full path with query string
    const mainUrl = `${MAIN_API_URL}${req.originalUrl}`;
    
    console.log(`   Proxying to: ${mainUrl}`);
    
    let response;
    if (req.method === 'GET') {
      response = await axios.get(mainUrl, {
        timeout: 30000
      });
    } else if (req.method === 'POST') {
      response = await axios.post(mainUrl, req.body, {
        headers: {
          'Content-Type': 'application/json'
        },
        timeout: 30000
      });
    } else if (req.method === 'PUT') {
      response = await axios.put(mainUrl, req.body, {
        headers: {
          'Content-Type': 'application/json'
        },
        timeout: 30000
      });
    } else if (req.method === 'DELETE') {
      response = await axios.delete(mainUrl, {
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

    // Forward the response from Main
    console.log(`   ✅ Proxy success: ${response.status}`);
    res.status(response.status).json(response.data);
    
  } catch (error) {
    console.error('❌ Error proxying to Main:', error.message);
    if (error.response) {
      console.error('   Response status:', error.response.status);
      console.error('   Response data:', error.response.data);
    }
    
    // If Main is also down, return error
    res.status(503).json({
      success: false,
      message: 'Service temporarily unavailable - both local database and Main node are unreachable',
      error: error.message,
      node: CURRENT_NODE,
      attemptedUrl: `${MAIN_API_URL}${req.originalUrl}`
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
  isDatabaseHealthy: () => isDatabaseHealthy
};
