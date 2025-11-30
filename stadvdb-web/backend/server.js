const express = require('express');
const cors = require('cors');
require('dotenv').config({ path: process.env.DOTENV_CONFIG_PATH || '.env' });
// Use simple direct database connection//
const db = require('./db');
const recovery = require('./recovery');
const failoverProxy = require('./failover-proxy');

const app = express();
const PORT = process.env.PORT || 5000;

/// Middleware//
// Updated CORS to allow Vercel deployments and all Render frontends
const corsOptions = {
  origin: [
    'http://localhost:3000',
    'http://localhost:60751',
    'http://localhost:60752',
    'http://localhost:60753',
    'https://stadvdb-mco2-main-node.onrender.com',
    'https://stadvdb-mco2-a-node.onrender.com',
    'https://stadvdb-mco2-b-node.onrender.com',
    /\.vercel\.app$/, // Allow all Vercel deployments
    /\.onrender\.com$/ // Allow all Render deployments
  ],
  credentials: true,
  optionsSuccessStatus: 200
};
app.use(cors(corsOptions));
app.use(express.json());

// Add debug headers to all responses
app.use((req, res, next) => {
  const currentNode = failoverProxy.getCurrentNode();
  res.set('X-Current-Node', currentNode);
  res.set('X-DB-Healthy', failoverProxy.isDatabaseHealthy() ? 'true' : 'false');
  next();
});

// Middleware to force Node B to always proxy to coordinator (Main or Node A)
app.use('/api', (req, res, next) => {
  const currentNode = failoverProxy.getCurrentNode();
  
  // Node B ALWAYS proxies to Main/Node A (acts as simple client, not coordinator)
  if (currentNode === 'NODE_B') {
    return failoverProxy.forwardToMain(req, res, next);
  }
  
  // Main and Node A use their own databases
  next();
});

// ============================================================================
// API ROUTES

// DISTRIBUTED TRANSACTIONS
// Distributed insert system
app.post('/api/titles/distributed-insert', async (req, res) => {
  try {
    const { tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear } = req.body;
    
    // Validate required fields
    if (!tconst || !primaryTitle || runtimeMinutes === undefined || averageRating === undefined || numVotes === undefined || startYear === undefined) {
      return res.status(400).json({ success: false, message: 'All fields are required' });
    }

    const sql = 'CALL distributed_insert(?, ?, ?, ?, ?, ?)';
    const params = [tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear];
    
    await db.query(sql, params);
    res.json({ success: true, message: 'Inserted successfully' });
    
  } catch (error) {
    console.error('Insert Error:', error);
    
    // Check if it's a connection error
    const isConnectionError = error.code === 'ETIMEDOUT' || 
                              error.code === 'ECONNREFUSED' ||
                              error.code === 'EHOSTUNREACH' ||
                              error.message?.includes('connect ETIMEDOUT') || 
                              error.message?.includes('connect ECONNREFUSED') ||
                              error.message?.includes('connect EHOSTUNREACH');
    
    if (isConnectionError) {
      // Try to proxy to another node
      return failoverProxy.forwardToMain(req, res, () => {
        res.status(500).json({ success: false, message: 'Insert Error - all nodes unavailable', error: error.message });
      });
    }
    
    res.status(500).json({ success: false, message: 'Insert Error', error: error.message });
  }
});

// Distributed update system
app.post('/api/titles/distributed-update', async (req, res) => {
  try {
    const { tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear } = req.body;
    
    if (!tconst) {
      return res.status(400).json({ success: false, message: 'tconst is required' });
    }

    const sql = 'CALL distributed_update(?, ?, ?, ?, ?, ?)';
    const params = [tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear];
    
    await db.query(sql, params);
    res.json({ success: true, message: 'Updated successfully' });
    
  } catch (error) {
    console.error('Update Error:', error);
    
    // Check if it's a connection error
    const isConnectionError = error.code === 'ETIMEDOUT' || 
                              error.code === 'ECONNREFUSED' ||
                              error.code === 'EHOSTUNREACH' ||
                              error.message?.includes('connect ETIMEDOUT') || 
                              error.message?.includes('connect ECONNREFUSED') ||
                              error.message?.includes('connect EHOSTUNREACH');
    
    if (isConnectionError) {
      // Try to proxy to another node
      return failoverProxy.forwardToMain(req, res, () => {
        res.status(500).json({ success: false, message: 'Update Error - all nodes unavailable', error: error.message });
      });
    }
    
    res.status(500).json({ success: false, message: 'Update Error', error: error.message });
  }
});

// Distributed delete system
app.post('/api/titles/distributed-delete', async (req, res) => {
  try {
    const { tconst } = req.body;
    
    if (!tconst) {
      return res.status(400).json({ success: false, message: 'tconst required' });
    }

    const sql = 'CALL distributed_delete(?)';
    const params = [tconst];
    
    await db.query(sql, params);
    res.json({ success: true, message: 'Deleted successfully' });
    
  } catch (error) {
    console.error('Delete Error:', error);
    
    // Check if it's a connection error
    const isConnectionError = error.code === 'ETIMEDOUT' || 
                              error.code === 'ECONNREFUSED' ||
                              error.code === 'EHOSTUNREACH' ||
                              error.message?.includes('connect ETIMEDOUT') || 
                              error.message?.includes('connect ECONNREFUSED') ||
                              error.message?.includes('connect EHOSTUNREACH');
    
    if (isConnectionError) {
      // Try to proxy to another node
      return failoverProxy.forwardToMain(req, res, () => {
        res.status(500).json({ success: false, message: 'Delete Error - all nodes unavailable', error: error.message });
      });
    }
    
    res.status(500).json({ success: false, message: 'Delete Error', error: error.message });
  }
});

// Add reviews system
app.post('/api/titles/add-reviews', async (req, res) => {
  try {
    const { tconst, newRating, newVotes } = req.body;
    
    if (!tconst || newRating === undefined || newVotes === undefined) {
      return res.status(400).json({ success: false, message: 'tconst, newRating, and newVotes required' });
    }

    // Validate rating range
    if (newRating < 0 || newRating > 10) {
      return res.status(400).json({ success: false, message: 'Rating must be between 0 and 10' });
    }

    // Validate votes is positive
    if (newVotes <= 0) {
      return res.status(400).json({ success: false, message: 'Number of votes must be positive' });
    }

    const sql = 'CALL distributed_addReviews(?, ?, ?)';
    const params = [tconst, newVotes, newRating]; // Order: tconst, num_new_reviews, new_rating
    
    await db.query(sql, params);
    res.json({ success: true, message: 'Reviews added successfully' });
    
  } catch (error) {
    console.error('Add Reviews Error:', error);
    
    // Check if it's a connection error
    const isConnectionError = error.code === 'ETIMEDOUT' || 
                              error.code === 'ECONNREFUSED' ||
                              error.code === 'EHOSTUNREACH' ||
                              error.message?.includes('connect ETIMEDOUT') || 
                              error.message?.includes('connect ECONNREFUSED') ||
                              error.message?.includes('connect EHOSTUNREACH');
    
    if (isConnectionError) {
      // Try to proxy to another node
      return failoverProxy.forwardToMain(req, res, () => {
        res.status(500).json({ success: false, message: 'Add Reviews Error - all nodes unavailable', error: error.message });
      });
    }
    
    res.status(500).json({ success: false, message: 'Add Reviews Error', error: error.message });
  }
});

// READ-ONLY ROUTES
// Distributed select system
app.get('/api/titles/distributed-select', async (req, res) => {
    try {
        const { select_column = 'averageRating', order_direction = 'DESC', limit_count = 10 } = req.query;
        const [results] = await db.query('CALL distributed_select(?, ?, ?)', [select_column, order_direction, parseInt(limit_count)]);
        res.json({ success: true, count: results[0]?.length || 0, data: results[0] || [] });
    } catch (error) {
        console.error('Error in select:', error);
        
        // Check if procedure doesn't exist (Node A/B) or connection error
        const procedureNotFound = error.code === 'ER_SP_DOES_NOT_EXIST' || 
                                  error.message?.includes('PROCEDURE') ||
                                  error.message?.includes('does not exist');
        const isConnectionError = error.code === 'ETIMEDOUT' || error.code === 'ECONNREFUSED' || 
                                  error.message?.includes('connect ETIMEDOUT') || 
                                  error.message?.includes('connect ECONNREFUSED') ||
                                  error.message?.includes('Unable to connect to foreign data source');
        
        // If procedure doesn't exist or connection error, use local select
        if (procedureNotFound || isConnectionError) {
          console.log('   Using local select (distributed procedure not available)');
          try {
            const { select_column = 'averageRating', order_direction = 'DESC', limit_count = 10 } = req.query;
            
            // Validate inputs
            const validColumns = ['primaryTitle', 'numVotes', 'averageRating', 'weightedRating', 'startYear', 'tconst', 'runtimeMinutes'];
            const column = validColumns.includes(select_column) ? select_column : 'averageRating';
            const direction = order_direction === 'ASC' ? 'ASC' : 'DESC';
            const limit = Math.min(Math.max(parseInt(limit_count) || 10, 1), 100);
            
            const [localResults] = await db.query(
              `SELECT * FROM title_ft ORDER BY ${column} ${direction} LIMIT ?`,
              [limit]
            );
            
            return res.json({ 
              success: true, 
              count: localResults?.length || 0, 
              data: localResults || [],
              source: 'local'
            });
          } catch (localError) {
            console.error('Error in local select:', localError);
            // Try proxying
            return failoverProxy.forwardToMain(req, res, () => {
              res.status(500).json({ message: 'Error in select', error: localError.message });
            });
          }
        }
        
        res.status(500).json({ message: 'Error in select', error: error.message });
    }
});

// Distributed search system
app.get('/api/titles/distributed-search', async (req, res) => {
  try {
    const { search_term = '', limit_count = 20 } = req.query;
    const [results] = await db.query('CALL distributed_search(?, ?)', [search_term, parseInt(limit_count)]);
    res.json({
      success: true,
      count: results[0]?.length || 0,
      data: results[0] || []
    });
  } catch (error) {
    console.error('Error in distributed_search:', error);
    
    // Check if it's a connection error
    const isConnectionError = error.code === 'ETIMEDOUT' || 
                              error.code === 'ECONNREFUSED' ||
                              error.code === 'EHOSTUNREACH' ||
                              error.message?.includes('connect ETIMEDOUT') || 
                              error.message?.includes('connect ECONNREFUSED') ||
                              error.message?.includes('connect EHOSTUNREACH');
    
    if (isConnectionError) {
      // Try to proxy to another node
      return failoverProxy.forwardToMain(req, res, () => {
        res.status(500).json({
          success: false,
          message: 'Error in distributed_search - all nodes unavailable',
          error: error.message
        });
      });
    }
    
    res.status(500).json({
      success: false,
      message: 'Error in distributed_search',
      error: error.message
    });
  }
});

// Aggregation Route
app.get('/api/aggregation', async (req, res) => {
  // Check if we should proxy first (Main with DB down, or always-proxy nodes)
  const proxyTarget = await failoverProxy.getProxyTarget();
  if (proxyTarget) {
    console.log(`🔄 Proactively proxying /api/aggregation to ${proxyTarget.name}`);
    return failoverProxy.forwardToMain(req, res, () => {
      // Fallback if proxy fails
      res.status(500).json({
        success: false,
        message: 'Error fetching aggregations - all nodes unavailable'
      });
    });
  }
  
  try {
    // Try to call distributed_aggregation if it exists (Main node)
    const [results] = await db.query('CALL distributed_aggregation()', []);
    let agg = results[0] && results[0][0] ? results[0][0] : null;
    if (agg) {
      agg.movie_count = Number(agg.movie_count);
      agg.average_rating = agg.average_rating !== null ? Number(agg.average_rating) : null;
      agg.average_weightedRating = agg.average_weightedRating !== null ? Number(agg.average_weightedRating) : null;
      agg.total_votes = agg.total_votes !== null ? Number(agg.total_votes) : null;
      agg.average_votes = agg.average_votes !== null ? Number(agg.average_votes) : null;
    }
    
    // Add debug info to response
    const currentNode = failoverProxy.getCurrentNode();
    res.set('X-Data-Source', 'distributed_aggregation');
    res.set('X-Served-By', currentNode);
    
    res.json({
      success: true,
      raw: results,
      data: agg,
      debug: {
        source: 'distributed_aggregation',
        servedBy: currentNode,
        movieCount: agg?.movie_count
      }
    });
  } catch (error) {
    console.error('Error fetching aggregations:', error);
    
    // Check if procedure doesn't exist (Node A/B) or connection error
    const procedureNotFound = error.code === 'ER_SP_DOES_NOT_EXIST' || 
                              error.message?.includes('PROCEDURE') ||
                              error.message?.includes('does not exist');
    const isConnectionError = error.code === 'ETIMEDOUT' || 
                              error.code === 'ECONNREFUSED' || 
                              error.message?.includes('connect ETIMEDOUT') || 
                              error.message?.includes('connect ECONNREFUSED') ||
                              error.message?.includes('Unable to connect to foreign data source');
    
    // If procedure doesn't exist or connection error, use local aggregation
    if (procedureNotFound || isConnectionError) {
      console.log('   Using local aggregation (distributed procedure not available)');
      try {
        const [localResults] = await db.query(`
          SELECT 
            COUNT(*) AS movie_count,
            AVG(averageRating) AS average_rating,
            AVG(weightedRating) AS average_weightedRating,
            SUM(numVotes) AS total_votes,
            AVG(numVotes) AS average_votes
          FROM title_ft
        `);
        
        let agg = localResults[0];
        if (agg) {
          agg.movie_count = Number(agg.movie_count);
          agg.average_rating = agg.average_rating !== null ? Number(agg.average_rating) : null;
          agg.average_weightedRating = agg.average_weightedRating !== null ? Number(agg.average_weightedRating) : null;
          agg.total_votes = agg.total_votes !== null ? Number(agg.total_votes) : null;
          agg.average_votes = agg.average_votes !== null ? Number(agg.average_votes) : null;
        }
        
        return res.json({
          success: true,
          data: agg,
          source: 'local' // Indicate this is local data
        });
      } catch (localError) {
        console.error('Error in local aggregation:', localError);
        // If local also fails, try proxying to another node
        return failoverProxy.forwardToMain(req, res, () => {
          res.status(500).json({
            success: false,
            message: 'Error fetching aggregations',
            error: localError.message
          });
        });
      }
    }
    
    res.status(500).json({
      success: false,
      message: 'Error fetching aggregations',
      error: error.message
    });
  }
});

// Advanced Search
app.get('/api/titles/search-advanced', async (req, res) => {
  try {
    const { title_query, min_rating, max_rating, min_votes, result_limit = 10 } = req.query;
    const query = `
      SELECT tf.tconst, tf.primaryTitle AS title, tf.averageRating, tf.numVotes
      FROM title_ft AS tf
      WHERE (? IS NULL OR tf.primaryTitle LIKE CONCAT('%', ?, '%'))
      AND (? IS NULL OR tf.averageRating >= ?)
      AND (? IS NULL OR tf.averageRating <= ?)
      AND (? IS NULL OR tf.numVotes >= ?)
      ORDER BY tf.numVotes DESC LIMIT ?
    `;
    const params = [title_query || null, title_query || null, min_rating || null, min_rating || null, max_rating || null, max_rating || null, min_votes || null, min_votes || null, parseInt(result_limit)];
    const [results] = await db.query(query, params);
    res.json({ success: true, count: results.length, data: results });
  } catch (error) {
    res.status(500).json({ message: 'Error searching titles', error: error.message });
  }
});

// Top movies of a time period
app.get('/api/titles/top-by-year', async (req, res) => {
  try {
    const {
      start_year = 2000,
      end_year = 2020,
      min_votes = 1000
    } = req.query;

    const query = `
      WITH ranked_titles AS (
        SELECT
          t.tconst,
          t.startYear,
          t.primaryTitle,
          t.averageRating,
          tb.genres,
          ROW_NUMBER() OVER (
            PARTITION BY t.startYear
            ORDER BY t.averageRating DESC
          ) AS rnk
        FROM title_ft t
        LEFT JOIN title_basics AS tb ON t.tconst = tb.tconst
        WHERE t.averageRating IS NOT NULL
          AND t.numVotes > ?
          AND t.startYear > ?
          AND t.startYear < ?
      )
      SELECT
        rt.startYear,
        rt.primaryTitle,
        rt.averageRating AS highest,
        rt.genres
      FROM ranked_titles rt
      WHERE rt.rnk = 1
      ORDER BY rt.startYear
    `;

    const params = [
      parseInt(min_votes),
      parseInt(start_year),
      parseInt(end_year)
    ];

    const [results] = await db.query(query, params);
    res.json({
      success: true,
      count: results.length,
      filters: {
        start_year,
        end_year,
        min_votes
      },
      data: results
    });
  } catch (error) {
    console.error('Error fetching top movies by year:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching top movies by year',
      error: error.message
    });
  }
});

// Test route
app.get('/api/test', (req, res) => {
  res.json({ message: 'Backend API is working!' });
});

// ============================================================================
// RECOVERY API ENDPOINTS
// ============================================================================

// Manual recovery trigger for Node A
app.post('/api/recovery/node-a', async (req, res) => {
  try {
    const { sinceTimestamp } = req.body;
    const result = await recovery.recoverNodeA(sinceTimestamp);
    res.json({ success: true, result });
  } catch (error) {
    console.error('Recovery API Error (Node A):', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Manual recovery trigger for Node B
app.post('/api/recovery/node-b', async (req, res) => {
  try {
    const { sinceTimestamp } = req.body;
    const result = await recovery.recoverNodeB(sinceTimestamp);
    res.json({ success: true, result });
  } catch (error) {
    console.error('Recovery API Error (Node B):', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Check for uncommitted transactions
app.get('/api/recovery/uncommitted', async (req, res) => {
  try {
    const result = await recovery.checkUncommittedTransactions();
    res.json({ success: true, result });
  } catch (error) {
    console.error('Recovery API Error (Uncommitted):', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get recovery status
app.get('/api/recovery/status', (req, res) => {
  res.json({
    success: true,
    node: process.env.DB_NAME || 'unknown',
    currentNode: failoverProxy.getCurrentNode(),
    isMain: recovery.isMainNode(),
    isNodeA: recovery.isNodeA(),
    isNodeB: recovery.isNodeB(),
    canRecover: recovery.isMainNode(),
    periodicRecoveryEnabled: recovery.isMainNode(),
    databaseHealthy: failoverProxy.isDatabaseHealthy(),
    failoverStatus: {
      mainHealthy: failoverProxy.isMainHealthy(),
      nodeAHealthy: failoverProxy.isNodeAHealthy(),
      nodeBHealthy: failoverProxy.isNodeBHealthy()
    }
  });
});

// ============================================================================
// ERROR HANDLERS
// ============================================================================

// Database error handler - catches connection errors and proxies to Main
app.use(failoverProxy.handleDatabaseError);

// Generic error handler
app.use((error, req, res, next) => {
  console.error('❌ Unhandled error:', error);
  res.status(500).json({
    success: false,
    message: 'Internal server error',
    error: error.message
  });
});

// ============================================================================
// START SERVER
// ============================================================================

app.listen(PORT, async () => {
  const DB_NAME = process.env.DB_NAME || 'stadvdb-mco2';
  const NODE_TYPE = DB_NAME === 'stadvdb-mco2' ? 'MAIN' : 
                    DB_NAME === 'stadvdb-mco2-a' ? 'NODE_A' : 
                    DB_NAME === 'stadvdb-mco2-b' ? 'NODE_B' : 'MAIN';
  
  console.log(`🚀 Server is running on http://localhost:${PORT}`);
  console.log(`📊 API endpoints available at http://localhost:${PORT}/api`);
  console.log(`🔧 Node type: ${NODE_TYPE}`);
  console.log(`🔄 Failover to Main: ${NODE_TYPE !== 'MAIN' ? 'ENABLED' : 'N/A (this is Main)'}`);
  
  // Run automatic recovery check on startup
  try {
    await recovery.runStartupRecovery();
  } catch (error) {
    console.error('❌ Error during startup recovery:', error.message);
    console.error('⚠️ Server will continue running, but recovery may be incomplete');
  }
  
  // Start periodic recovery checks (every 5 minutes)
  recovery.startPeriodicRecovery(5);
});