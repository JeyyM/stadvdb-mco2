const mysql = require('mysql2');
require('dotenv').config();

// Define all database nodes in priority order
const DB_NODES = [
  {
    name: 'MAIN',
    host: process.env.DB_HOST_MAIN || 'stadvdb-mco2.h.filess.io',
    port: process.env.DB_PORT_MAIN || 3307,
    user: process.env.DB_USER || 'g18',
    password: process.env.DB_PASSWORD || 'fuckingpassword',
    database: process.env.DB_NAME_MAIN || 'stadvdb-mco2',
    writeCapable: true // Can handle writes
  },
  {
    name: 'NODE_A',
    host: process.env.DB_HOST_A || 'stadvdb-mco2-a.h.filess.io',
    port: process.env.DB_PORT_A || 3307,
    user: process.env.DB_USER || 'g18',
    password: process.env.DB_PASSWORD || 'fuckingpassword',
    database: process.env.DB_NAME_A || 'stadvdb-mco2-a',
    writeCapable: false // Only writes if in ACTING_MASTER mode (will be checked dynamically)
  },
  {
    name: 'NODE_B',
    host: process.env.DB_HOST_B || 'stadvdb-mco2-b.h.filess.io',
    port: process.env.DB_PORT_B || 3307,
    user: process.env.DB_USER || 'g18',
    password: process.env.DB_PASSWORD || 'fuckingpassword',
    database: process.env.DB_NAME_B || 'stadvdb-mco2-b',
    writeCapable: false // Read-only replica
  }
];

// Create connection pools for all nodes
const pools = {};
DB_NODES.forEach(node => {
  pools[node.name] = mysql.createPool({
    host: node.host,
    user: node.user,
    password: node.password,
    database: node.database,
    port: node.port,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 5000, // 5 second timeout
    enableKeepAlive: true,
    keepAliveInitialDelay: 10000
  }).promise();
});

// Track which nodes are currently available
const nodeStatus = {
  MAIN: { available: false, lastChecked: null, isActingMaster: false },
  NODE_A: { available: false, lastChecked: null, isActingMaster: false },
  NODE_B: { available: false, lastChecked: null, isActingMaster: false }
};

// Check if a node is available
async function checkNodeHealth(nodeName) {
  try {
    const pool = pools[nodeName];
    await pool.query('SELECT 1');
    nodeStatus[nodeName].available = true;
    nodeStatus[nodeName].lastChecked = new Date();
    
    // For Node A, check if it's in ACTING_MASTER mode
    if (nodeName === 'NODE_A') {
      try {
        const [rows] = await pool.query(
          "SELECT config_value FROM node_config WHERE config_key = 'node_mode'"
        );
        nodeStatus[nodeName].isActingMaster = rows[0]?.config_value === 'ACTING_MASTER';
      } catch (err) {
        // node_config table might not exist, assume VICE mode
        nodeStatus[nodeName].isActingMaster = false;
      }
    }
    
    console.log(`✓ ${nodeName} is healthy ${nodeName === 'NODE_A' && nodeStatus[nodeName].isActingMaster ? '(ACTING_MASTER)' : ''}`);
    return true;
  } catch (error) {
    nodeStatus[nodeName].available = false;
    nodeStatus[nodeName].lastChecked = new Date();
    console.log(`✗ ${nodeName} is unavailable:`, error.message);
    return false;
  }
}

// Health check all nodes periodically
async function healthCheckAll() {
  console.log('\n--- Health Check ---');
  for (const node of DB_NODES) {
    await checkNodeHealth(node.name);
  }
  console.log('Status:', nodeStatus);
}

// Start periodic health checks (every 10 seconds)
setInterval(healthCheckAll, 10000);

// Initial health check
healthCheckAll();

// Get the best available node for reads
function getBestReadNode() {
  // Priority: Use the node specified in .env, fallback to MAIN, then NODE_A, then NODE_B
  const preferredNode = process.env.DB_PREFERRED_NODE || 'MAIN';
  
  if (nodeStatus[preferredNode]?.available) {
    return preferredNode;
  }
  
  // Fallback chain
  if (nodeStatus.MAIN.available) return 'MAIN';
  if (nodeStatus.NODE_A.available) return 'NODE_A';
  if (nodeStatus.NODE_B.available) return 'NODE_B';
  
  throw new Error('All database nodes are unavailable');
}

// Get the best available node for writes
function getBestWriteNode() {
  // For writes, priority is: MAIN -> NODE_A (if ACTING_MASTER) -> fail
  // IMPORTANT: Main node stored procedures have error handlers that allow
  // operations to succeed even when Node A/B are down. The transaction logs
  // will be used for recovery when nodes come back online.
  if (nodeStatus.MAIN.available) {
    return 'MAIN';
  }
  
  if (nodeStatus.NODE_A.available && nodeStatus.NODE_A.isActingMaster) {
    console.warn('⚠️  Using NODE_A as acting master (Main is down)');
    return 'NODE_A';
  }
  
  throw new Error('No write-capable database node available (Main is down and Node A is not in ACTING_MASTER mode)');
}

// Smart query executor with automatic failover
async function executeQuery(sql, params = [], options = {}) {
  const isWrite = options.isWrite || false;
  const nodeToUse = isWrite ? getBestWriteNode() : getBestReadNode();
  
  try {
    console.log(`Executing ${isWrite ? 'WRITE' : 'READ'} on ${nodeToUse}`);
    const result = await pools[nodeToUse].query(sql, params);
    return result;
  } catch (error) {
    console.error(`Query failed on ${nodeToUse}:`, error.message);
    
    // If this was the preferred node and it failed, mark it as unavailable
    nodeStatus[nodeToUse].available = false;
    
    // Try fallback
    if (!isWrite) {
      // For reads, try other nodes
      const fallbackNodes = DB_NODES
        .map(n => n.name)
        .filter(n => n !== nodeToUse && nodeStatus[n].available);
      
      for (const fallbackNode of fallbackNodes) {
        try {
          console.log(`Trying fallback node: ${fallbackNode}`);
          const result = await pools[fallbackNode].query(sql, params);
          return result;
        } catch (fallbackError) {
          console.error(`Fallback to ${fallbackNode} failed:`, fallbackError.message);
          nodeStatus[fallbackNode].available = false;
        }
      }
    }
    
    // If we get here, all attempts failed
    throw new Error(`Query failed on all available nodes. Last error: ${error.message}`);
  }
}

// Export a db interface that mimics the original db.js
module.exports = {
  query: executeQuery,
  
  // For backward compatibility
  getConnection: async () => {
    const nodeToUse = getBestReadNode();
    return pools[nodeToUse].getConnection();
  },
  
  // Expose node status for monitoring
  getNodeStatus: () => nodeStatus,
  
  // Manual health check trigger
  checkHealth: healthCheckAll,
  
  // Get specific pool (for advanced usage)
  getPool: (nodeName) => pools[nodeName]
};
