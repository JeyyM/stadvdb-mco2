/**
 * Failover Proxy Middleware
 *
 * Pls read the goal:
 * - MAIN node fails over to Node A if its own DB is unavailable
 * - Node A / Node B can proxy to MAIN (and each other) as backup
 *
 * Frontend:
 *   https://stadvdb-mco2-main-node.onrender.com/
 *   https://stadvdb-mco2-a-node.onrender.com
 *   https://stadvdb-mco2-b-node.onrender.com
 *
 * Backend:
 *   https://stadvdb-mco2-main.onrender.com
 *   https://stadvdb-mco2-a.onrender.com
 *   https://stadvdb-mco2-b.onrender.com
 */

const axios = require('axios');
const db = require('./db');

const DB_NAME = process.env.DB_NAME || 'stadvdb-mco2';
const CURRENT_NODE =
  DB_NAME === 'stadvdb-mco2'
    ? 'MAIN'
    : DB_NAME === 'stadvdb-mco2-a'
    ? 'NODE_A'
    : DB_NAME === 'stadvdb-mco2-b'
    ? 'NODE_B'
    : 'MAIN';

// API URLs, don't touch
const MAIN_API_URL = 'https://stadvdb-mco2-main.onrender.com';
const NODE_A_API_URL = 'https://stadvdb-mco2-a.onrender.com';
const NODE_B_API_URL = 'https://stadvdb-mco2-b.onrender.com';

let isDatabaseHealthy = true;
let lastHealthCheck = Date.now();
const HEALTH_CHECK_INTERVAL = 30000; // this is 30s

let mainNodeHealthy = true;
let nodeAHealthy = true;
let nodeBHealthy = true;
let lastRemoteHealthCheck = 0;
const REMOTE_HEALTH_CHECK_INTERVAL = 15000; // this is 15s

/**
 * Helper: detect DB / connection errors, don't touch
 */
function isConnectionError(error) {
  return (
    error?.code === 'ECONNREFUSED' ||
    error?.code === 'ETIMEDOUT' ||
    error?.code === 'ENOTFOUND' ||
    error?.errno === 'ECONNREFUSED' ||
    error?.sqlState === 'HY000' ||
    (typeof error?.message === 'string' &&
      (error.message.includes('connect ETIMEDOUT') ||
        error.message.includes('connect ECONNREFUSED')))
  );
}

/**
 * Check if the *local* database connection is healthy/
 */
async function checkDatabaseHealth() {
  const now = Date.now();
  if (now - lastHealthCheck < 1000) {
    return isDatabaseHealthy;
  }
  lastHealthCheck = now;

  try {
    await db.query('SELECT 1');
    if (!isDatabaseHealthy) {
      console.log(`DB on ${CURRENT_NODE} is healthy again`);
    }
    isDatabaseHealthy = true;
    return true;
  } catch (error) {
    if (isDatabaseHealthy) {
      console.error(`DB health check failed on ${CURRENT_NODE}:`, error.message);
    }
    isDatabaseHealthy = false;
    return false;
  }
}

/**
 * Check health of remote nodes via /api/recovery/status
 */
async function checkRemoteNodeHealth() {
  const now = Date.now();
  if (now - lastRemoteHealthCheck < REMOTE_HEALTH_CHECK_INTERVAL) {
    return;
  }
  lastRemoteHealthCheck = now;

  // Helper to ping a node
  async function pingNode(name, url, setHealthy) {
    try {
      const response = await axios.get(`${url}/api/recovery/status`, { timeout: 5000 });
      // Check if the node's DATABASE is healthy, not just if HTTP is responding
      const dbHealthy = response.data?.databaseHealthy === true;
      setHealthy(dbHealthy);
      if (!dbHealthy) {
        console.log(`${name} HTTP is up but database is down`);
      }
    } catch (error) {
      setHealthy(false);
      console.log(`${name} appears to be down: ${error.message}`);
    }
  }

  // MAIN node health (for A/B)
  if (CURRENT_NODE !== 'MAIN') {
    await pingNode('Main node', MAIN_API_URL, (v) => (mainNodeHealthy = v));
  }

  // Node A health (for MAIN and B)
  if (CURRENT_NODE !== 'NODE_A') {
    await pingNode('Node A', NODE_A_API_URL, (v) => (nodeAHealthy = v));
  }

  // Node B health (for MAIN and A)
  if (CURRENT_NODE !== 'NODE_B') {
    await pingNode('Node B', NODE_B_API_URL, (v) => (nodeBHealthy = v));
  }
}

/**
 * Determine which node to proxy to, based on failover rules.
 *
 *  MAIN: Prefer local DB, If DB is DOWN = try Node A, then Node B
 *
 * NODE_A: Prefer MAIN (coordinator) if healthy, Otherwise handle locally
 *
 * NODE_B: Prefer MAIN,  If MAIN down = Node A, Otherwise local
 */
async function getProxyTarget() {
  if (CURRENT_NODE === 'MAIN') {
    const dbHealthy = await checkDatabaseHealth();
    if (!dbHealthy) {
      if (nodeAHealthy) {
        console.log(`Main DB down, targeting Node A for failover`);
        return { url: NODE_A_API_URL, name: 'Node A (failover from Main)' };
      }
      if (nodeBHealthy) {
        console.log(`Main DB down and Node A unavailable, targeting Node B for failover`);
        return { url: NODE_B_API_URL, name: 'Node B (secondary failover from Main)' };
      }
    }
    // DB is healthy or no backup nodes ,then use local MAIN
    return null;
  }

  // NODE_B: follow Main = Node A = local
  if (CURRENT_NODE === 'NODE_B') {
    if (mainNodeHealthy) {
      return { url: MAIN_API_URL, name: 'Main' };
    } else if (nodeAHealthy) {
      return { url: NODE_A_API_URL, name: 'Node A' };
    }
    return null; // no proxy available, use local DB
  }

  // NODE_A: follow Main = local (A is second in hierarchy)
  if (CURRENT_NODE === 'NODE_A') {
    if (mainNodeHealthy) {
      return { url: MAIN_API_URL, name: 'Main' };
    }
    return null; // noo proxy available, Node A will act as backup coordinator
  }

  return null;
}
setInterval(async () => {
  await checkDatabaseHealth();
  await checkRemoteNodeHealth();
}, HEALTH_CHECK_INTERVAL);

/**
 * Forward request to another node based on failover hierarchy
 *
 * IMPORTANT PLS READ TYSM:
 * - If there is NO proxy target, we call next() so the local node handles it
 * - If proxying succeeds, this function sends the response and returns
 * - If all proxies fail and local DB is healthy, we fall back to next()
 * - If everything is down = 503
 */
async function forwardToMain(req, res, next) {
  await checkRemoteNodeHealth();

  const proxyTarget = await getProxyTarget();

  if (!proxyTarget) {
    return next();
  }

  console.log(
    `${CURRENT_NODE} proxying ${req.method} ${req.originalUrl} to ${proxyTarget.name}`
  );

  const targetUrl = `${proxyTarget.url}${req.originalUrl}`;

  try {
    let response;

    if (req.method === 'GET') {
      response = await axios.get(targetUrl, {
        timeout: 30000
      });
    } else if (req.method === 'POST') {
      response = await axios.post(targetUrl, req.body, {
        headers: { 'Content-Type': 'application/json' },
        timeout: 30000
      });
    } else if (req.method === 'PUT') {
      response = await axios.put(targetUrl, req.body, {
        headers: { 'Content-Type': 'application/json' },
        timeout: 30000
      });
    } else if (req.method === 'DELETE') {
      response = await axios.delete(targetUrl, {
        data: req.body,
        headers: { 'Content-Type': 'application/json' },
        timeout: 30000
      });
    } else {
      return res.status(405).json({
        success: false,
        message: 'Method not supported for proxy'
      });
    }

    console.log(`   Proxy success: ${response.status} from ${proxyTarget.name}`);
    
    res.set('X-Proxied-From', CURRENT_NODE);
    res.set('X-Proxied-To', proxyTarget.name);
    res.set('X-Proxy-Success', 'true');
    
    return res.status(response.status).json(response.data);
  } catch (error) {
    console.error(`Error proxying to ${proxyTarget.name}:`, error.message);

    if (proxyTarget.url === MAIN_API_URL) {
      mainNodeHealthy = false;
    } else if (proxyTarget.url === NODE_A_API_URL) {
      nodeAHealthy = false;
    } else if (proxyTarget.url === NODE_B_API_URL) {
      nodeBHealthy = false;
    }

    const backupTarget = await getProxyTarget();
    if (backupTarget) {
      console.log(
        `${CURRENT_NODE} retrying proxy to ${backupTarget.name} for ${req.method} ${req.originalUrl}`
      );
      try {
        const backupUrl = `${backupTarget.url}${req.originalUrl}`;
        let response;

        if (req.method === 'GET') {
          response = await axios.get(backupUrl, { timeout: 30000 });
        } else if (req.method === 'POST') {
          response = await axios.post(backupUrl, req.body, {
            headers: { 'Content-Type': 'application/json' },
            timeout: 30000
          });
        } else if (req.method === 'PUT') {
          response = await axios.put(backupUrl, req.body, {
            headers: { 'Content-Type': 'application/json' },
            timeout: 30000
          });
        } else if (req.method === 'DELETE') {
          response = await axios.delete(backupUrl, {
            data: req.body,
            headers: { 'Content-Type': 'application/json' },
            timeout: 30000
          });
        }

        console.log(`   Retry success: ${response.status} from ${backupTarget.name}`);
        
        // Add debug headers for retry
        res.set('X-Proxied-From', CURRENT_NODE);
        res.set('X-Proxied-To', backupTarget.name);
        res.set('X-Proxy-Retry', 'true');
        res.set('X-Original-Target-Failed', proxyTarget.name);
        
        return res.status(response.status).json(response.data);
      } catch (backupError) {
        console.error(
          `Retry to ${backupTarget.name} also failed:`,
          backupError.message
        );
        if (backupTarget.url === MAIN_API_URL) {
          mainNodeHealthy = false;
        } else if (backupTarget.url === NODE_A_API_URL) {
          nodeAHealthy = false;
        } else if (backupTarget.url === NODE_B_API_URL) {
          nodeBHealthy = false;
        }
      }
    }

    const dbHealthy = await checkDatabaseHealth();
    if (dbHealthy) {
      console.log(
        `All remote nodes down or unreachable, falling back to local ${CURRENT_NODE} data`
      );
      res.set('X-Fallback-Mode', 'local');
      res.set('X-Current-Node', CURRENT_NODE);
      res.set('X-Remote-Nodes-Down', 'true');
      return next();
    }

    return res.status(503).json({
      success: false,
      message: 'Service temporarily unavailable - all nodes unreachable',
      error: error.message,
      node: CURRENT_NODE,
      debug: {
        currentNode: CURRENT_NODE,
        attemptedProxy: proxyTarget.name,
        backupAttempted: backupTarget ? backupTarget.name : 'none',
        localDbHealthy: dbHealthy,
        mainNodeHealthy,
        nodeAHealthy,
        nodeBHealthy
      }
    });
  }
}

 // Error handler that catches database connection errors and retries via proxy
function handleDatabaseError(error, req, res, next) {
  if (isConnectionError(error)) {
    console.log(
      `Database connection error detected on ${CURRENT_NODE}, marking DB unhealthy and attempting proxy`
    );
    isDatabaseHealthy = false;

    return forwardToMain(req, res, next);
  }

  return next(error);
}

 // Execute database query with automatic failover to proxy
async function queryWithFailover(sql, params, req, res) {
  try {
    const result = await db.query(sql, params);
    isDatabaseHealthy = true;
    return { success: true, proxied: false, result };
  } catch (error) {
    if (!isConnectionError(error)) {
      throw error;
    }

    console.log(
      `DB connection failed on ${CURRENT_NODE} during query, attempting proxy`
    );
    isDatabaseHealthy = false;
  }

  // Local DB is down, so proxy instead
  await forwardToMain(req, res, () => {});
  return { success: false, proxied: true };
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