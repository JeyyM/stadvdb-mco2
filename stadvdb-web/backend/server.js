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
// Updated CORS to allow Vercel deployments
const corsOptions = {
  origin: [
    'http://localhost:3000',
    'http://localhost:60751',
    'http://localhost:60752',
    'http://localhost:60753',
    /\.vercel\.app$/, // Allow all Vercel deployments
    /\.onrender\.com$/ // Allow Render deployments
  ],
  credentials: true,
  optionsSuccessStatus: 200
};
app.use(cors(corsOptions));
app.use(express.json());

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
        
        // Check if it's a connection error - proxy to Main
        const isConnectionError = error.code === 'ETIMEDOUT' || error.code === 'ECONNREFUSED' || 
                                  error.message?.includes('connect ETIMEDOUT') || 
                                  error.message?.includes('connect ECONNREFUSED');
        
        if (isConnectionError) {
          return failoverProxy.forwardToMain(req, res, () => {
            res.status(500).json({ message: 'Error in select', error: error.message });
          });
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
    res.status(500).json({
      success: false,
      message: 'Error in distributed_search',
      error: error.message
    });
  }
});

// Aggregation Route
app.get('/api/aggregation', async (req, res) => {
  try {
    const [results] = await db.query('CALL distributed_aggregation()', []);
    let agg = results[0] && results[0][0] ? results[0][0] : null;
    if (agg) {
      agg.movie_count = Number(agg.movie_count);
      agg.average_rating = agg.average_rating !== null ? Number(agg.average_rating) : null;
      agg.average_weightedRating = agg.average_weightedRating !== null ? Number(agg.average_weightedRating) : null;
      agg.total_votes = agg.total_votes !== null ? Number(agg.total_votes) : null;
      agg.average_votes = agg.average_votes !== null ? Number(agg.average_votes) : null;
    }
    res.json({
      success: true,
      raw: results,
      data: agg
    });
  } catch (error) {
    console.error('Error fetching aggregations:', error);
    
    // Check if it's a connection error and we're not Main
    const isConnectionError = error.code === 'ETIMEDOUT' || error.code === 'ECONNREFUSED' || 
                              error.message?.includes('connect ETIMEDOUT') || 
                              error.message?.includes('connect ECONNREFUSED');
    
    if (isConnectionError) {
      return failoverProxy.forwardToMain(req, res, () => {
        res.status(500).json({
          success: false,
          message: 'Error fetching aggregations',
          error: error.message
        });
      });
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