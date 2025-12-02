const express = require('express');
const cors = require('cors');
require('dotenv').config({ path: process.env.DOTENV_CONFIG_PATH || '.env' });
// Use failover-capable database connection
const db = require('./db-failover');

const app = express();
const PORT = process.env.PORT || 5000;

/// Middleware
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
    
    await db.query(sql, params, { isWrite: true }); // Mark as write operation
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
    
    await db.query(sql, params, { isWrite: true }); // Mark as write operation
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
    
    await db.query(sql, params, { isWrite: true }); // Mark as write operation
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
    
    await db.query(sql, params, { isWrite: true }); // Mark as write operation
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
        const [results] = await db.query('CALL distributed_select(?, ?, ?)', [select_column, order_direction, parseInt(limit_count)], { isWrite: false }); // Mark as read
        res.json({ success: true, count: results[0]?.length || 0, data: results[0] || [] });
    } catch (error) {
        res.status(500).json({ message: 'Error in select', error: error.message });
    }
});

// Distributed search system
app.get('/api/titles/distributed-search', async (req, res) => {
  try {
    const { search_term = '', limit_count = 20 } = req.query;
    const [results] = await db.query('CALL distributed_search(?, ?)', [search_term, parseInt(limit_count)], { isWrite: false }); // Mark as read
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
    const [results] = await db.query('CALL distributed_aggregation()', [], { isWrite: false }); // Mark as read
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

// Health status endpoint - shows which database nodes are available
app.get('/api/db-status', (req, res) => {
  const status = db.getNodeStatus();
  res.json({
    success: true,
    nodes: status,
    summary: {
      totalNodes: Object.keys(status).length,
      availableNodes: Object.values(status).filter(n => n.available).length,
      mainAvailable: status.MAIN?.available || false,
      nodeAActingMaster: status.NODE_A?.isActingMaster || false
    }
  });
});

// Manual health check trigger
app.post('/api/db-check-health', async (req, res) => {
  try {
    await db.checkHealth();
    res.json({ success: true, message: 'Health check completed', status: db.getNodeStatus() });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Health check failed', error: error.message });
  }
});

// Manual recovery trigger - allows triggering recovery via API
app.post('/api/recovery/sync-from-main', async (req, res) => {
  try {
    console.log('🔄 Manual recovery requested: sync_from_main()');
    
    // Check if this is Node A or Node B
    const dbName = process.env.DB_NAME || process.env.DB_NAME_MAIN;
    if (dbName === 'stadvdb-mco2') {
      return res.status(400).json({ 
        success: false, 
        message: 'This endpoint is only for Node A or Node B. Main node should use /api/recovery/recover-from-node-a' 
      });
    }
    
    const [syncResult] = await db.query('CALL sync_from_main()', [], { isWrite: true });
    
    if (syncResult && syncResult[0] && syncResult[0].length > 0) {
      const sync = syncResult[0][0];
      res.json({
        success: true,
        message: 'Recovery completed successfully',
        result: {
          inserted: sync.records_inserted || 0,
          updated: sync.records_updated || 0,
          removed: sync.records_removed || 0,
          status: sync.status
        }
      });
    } else {
      res.json({ success: true, message: 'Recovery completed' });
    }
  } catch (error) {
    console.error('Recovery error:', error);
    res.status(500).json({ 
      success: false, 
      message: 'Recovery failed', 
      error: error.message 
    });
  }
});

// Manual Main node recovery from Node A
app.post('/api/recovery/recover-from-node-a', async (req, res) => {
  try {
    console.log('🔄 Manual Main recovery requested: recover_from_node_a()');
    
    // Check if this is Main node
    const dbName = process.env.DB_NAME || process.env.DB_NAME_MAIN;
    if (dbName !== 'stadvdb-mco2') {
      return res.status(400).json({ 
        success: false, 
        message: 'This endpoint is only for Main node. Node A/B should use /api/recovery/sync-from-main' 
      });
    }
    
    const [recoveryResult] = await db.query('CALL recover_from_node_a()', [], { isWrite: true });
    
    res.json({
      success: true,
      message: 'Main node recovery completed. Remember to call demote_to_vice() on Node A.',
      result: recoveryResult
    });
  } catch (error) {
    console.error('Main recovery error:', error);
    res.status(500).json({ 
      success: false, 
      message: 'Main node recovery failed', 
      error: error.message 
    });
  }
});

// Check if recovery is needed
app.get('/api/recovery/check-status', async (req, res) => {
  try {
    const [healthResult] = await db.query('CALL health_check()', [], { isWrite: false });
    
    if (healthResult && healthResult[0] && healthResult[0].length > 0) {
      const health = healthResult[0][0];
      const needsRecovery = Math.abs(health.record_count_difference || 0) > 5 || (health.records_only_in_main || 0) > 0;
      
      res.json({
        success: true,
        needsRecovery,
        health: {
          localRecords: health.local_count,
          mainRecords: health.main_count_in_partition,
          difference: health.record_count_difference,
          missingFromLocal: health.records_only_in_main,
          extraInLocal: health.records_only_in_local
        }
      });
    } else {
      res.json({ success: true, needsRecovery: false });
    }
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      message: 'Could not check recovery status', 
      error: error.message 
    });
  }
});

// ============================================================================
// AUTOMATIC RECOVERY ON STARTUP
// ============================================================================

async function performStartupRecovery() {
  try {
    console.log('🔄 Checking if recovery is needed...');
    
    // Determine which node this backend is connected to
    const dbName = process.env.DB_NAME || process.env.DB_NAME_MAIN;
    const isMainNode = dbName === 'stadvdb-mco2';
    const isNodeA = dbName === 'stadvdb-mco2-a';
    const isNodeB = dbName === 'stadvdb-mco2-b';
    
    if (isNodeA || isNodeB) {
      const nodeName = isNodeA ? 'Node A' : 'Node B';
      console.log(`📡 ${nodeName} detected - checking if recovery from Main is needed...`);
      
      try {
        // Check if this node needs recovery by calling health_check
        const [healthResult] = await db.query('CALL health_check()', [], { isWrite: false });
        
        if (healthResult && healthResult[0] && healthResult[0].length > 0) {
          const health = healthResult[0][0];
          const recordDiff = Math.abs(health.record_count_difference || 0);
          const recordsOnlyInMain = health.records_only_in_main || 0;
          
          // If significant difference, run recovery
          if (recordDiff > 5 || recordsOnlyInMain > 0) {
            console.log(`⚠️  ${nodeName} is out of sync (diff: ${recordDiff}, missing: ${recordsOnlyInMain})`);
            console.log(`🔧 Running automatic recovery: sync_from_main()...`);
            
            const [syncResult] = await db.query('CALL sync_from_main()', [], { isWrite: true });
            
            if (syncResult && syncResult[0] && syncResult[0].length > 0) {
              const sync = syncResult[0][0];
              console.log(`✅ Recovery complete!`);
              console.log(`   - Inserted: ${sync.records_inserted || 0} records`);
              console.log(`   - Updated: ${sync.records_updated || 0} records`);
              console.log(`   - Removed: ${sync.records_removed || 0} records`);
            }
          } else {
            console.log(`✅ ${nodeName} is in sync with Main (diff: ${recordDiff})`);
          }
        }
      } catch (recoveryError) {
        console.warn(`⚠️  Could not perform automatic recovery: ${recoveryError.message}`);
        console.log('   This is normal if Main node is unreachable');
      }
      
    } else if (isMainNode) {
      console.log('🏛️  Main node detected - checking if recovery from Node A is needed...');
      
      try {
        // Check if Main was down and Node A was acting master
        // This is more complex - would need to check Node A's logs
        // For now, we'll skip automatic Main recovery and require manual intervention
        console.log('   Main node recovery requires manual execution of recover_from_node_a()');
        console.log('   Run: CALL recover_from_node_a(); if Main was previously down');
      } catch (err) {
        console.warn(`   Could not check Main recovery status: ${err.message}`);
      }
    }
    
    console.log('✅ Startup recovery check complete\n');
    
  } catch (error) {
    console.error('❌ Error during startup recovery check:', error.message);
    console.log('   Server will continue starting, but data may be out of sync');
  }
}

// ============================================================================
// START SERVER
// ============================================================================

app.listen(PORT, async () => {
  console.log(`🚀 Server is running on http://localhost:${PORT}`);
  console.log(`📊 API endpoints available at http://localhost:${PORT}/api`);
  console.log(`🏥 Database health status: http://localhost:${PORT}/api/db-status\n`);
  
  // Perform automatic recovery check after server starts
  await performStartupRecovery();
});