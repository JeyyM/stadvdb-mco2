const express = require('express');
const cors = require('cors');
require('dotenv').config({ path: process.env.DOTENV_CONFIG_PATH || '.env' });
const db = require('./db');
const RecoveryManager = require('./recoveryManager');

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// ============================================================================
// API ROUTES

// 1. RECOVERY & LOGGING ENDPOINTS (Yazan sectio)
// Trigger the recovery process manually
// Usage: POST /api/recovery/sync with body { "node": "NODE_A" }
app.post('/api/recovery/sync', async (req, res) => {
  try {
    const { node } = req.body; // Expects 'NODE_A' or 'NODE_B'
    
    if (!node) {
      return res.status(400).json({ success: false, message: 'Node name required (NODE_A or NODE_B)' });
    }

    console.log(`Manual recovery triggered for ${node}`);
    await RecoveryManager.recoverFailedTransactions(node);
    
    res.json({ success: true, message: `Recovery process finished for ${node}` });
  } catch (error) {
    console.error('Error triggering recovery:', error);
    res.status(500).json({ success: false, message: 'Recovery failed', error: error.message });
  }
});

// 2. DISTRIBUTED TRANSACTIONS (Modified with Recovery Logic)
// Distributed insert system
app.post('/api/titles/distributed-insert', async (req, res) => {
  try {
    const {
      tconst,
      primaryTitle,
      runtimeMinutes,
      averageRating,
      numVotes,
      startYear
    } = req.body;

    // Validate required fields
    if (!tconst || !primaryTitle || runtimeMinutes === undefined || averageRating === undefined || numVotes === undefined || startYear === undefined) {
      return res.status(400).json({ success: false, message: 'All fields are required' });
    }

    // --- RECOVERY LOGIC START ---
    const currentNode = process.env.DB_NAME; 
    const isMainNode = currentNode === 'stadvdb-mco2';

    let firstWriteTarget = 'LOCAL';
    if (isMainNode) {
        firstWriteTarget = (startYear < 2010) ? 'NODE_B' : 'NODE_A';
    }

    const sql = 'CALL distributed_insert(?, ?, ?, ?, ?, ?)';
    const params = [tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear];

    const transactionId = await RecoveryManager.logTransaction('INSERT', firstWriteTarget, sql, params);

    try {
        const [results] = await db.query(sql, params);

        if (transactionId) await RecoveryManager.updateLogStatus(transactionId, 'COMMITTED');

        if (!isMainNode) {
            console.log(`[Sync] Local write successful on ${currentNode}. Attempting to sync to MAIN...`);
            
            const syncSql = 'CALL `stadvdb-mco2`.distributed_insert(?, ?, ?, ?, ?, ?)';
            
            const syncLogId = await RecoveryManager.logTransaction('INSERT', 'MAIN', syncSql, params);

            try {
                await db.query(syncSql, params);
                if (syncLogId) await RecoveryManager.updateLogStatus(syncLogId, 'COMMITTED');
                console.log(`[Sync] Successfully synced to MAIN.`);
            } catch (mainError) {
                console.error(`[Sync] Failed to sync to MAIN: ${mainError.message}`);
                if (syncLogId) await RecoveryManager.updateLogStatus(syncLogId, 'FAILED', mainError.message);
            }
        }

        res.json({ success: true, data: results });

    } catch (dbError) {
        console.error(`Transaction ${transactionId} failed. Logging error.`);
        if (transactionId) await RecoveryManager.updateLogStatus(transactionId, 'FAILED', dbError.message);
        throw dbError; 
    }
    // --- RECOVERY LOGIC END ---
  } catch (error) {
    console.error('Error in distributed_insert:', error);
    res.status(500).json({
      success: false,
      message: 'Error in distributed_insert',
      error: error.message
    });
  }
});

// Distributed update system
app.post('/api/titles/distributed-update', async (req, res) => {
  try {
    const { tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear } = req.body;

    if (!tconst) return res.status(400).json({ success: false, message: 'tconst is required' });

    // --- RECOVERY LOGIC START (YAZAN) ---
    const currentNode = process.env.DB_NAME; 
    const isMainNode = currentNode === 'stadvdb-mco2';

    // 1. Determine Local Target
    // If Main, we calculate based on year. If Node A/B, we write to LOCAL.
    let firstWriteTarget = 'LOCAL';
    if (isMainNode) {
        firstWriteTarget = (startYear < 2010) ? 'NODE_B' : 'NODE_A';
    }

    const sql = 'CALL distributed_update(?, ?, ?, ?, ?, ?)';
    const params = [tconst, primaryTitle, runtimeMinutes, averageRating, numVotes, startYear];

    // 2. Log Primary Transaction
    const transactionId = await RecoveryManager.logTransaction('UPDATE', firstWriteTarget, sql, params);

    try {
        // 3. Execute Primary (Local) Write
        const [results] = await db.query(sql, params);
        if (transactionId) await RecoveryManager.updateLogStatus(transactionId, 'COMMITTED');

        // 4. SYNC UPWARD (Node A/B -> Main)
        if (!isMainNode) {
            console.log(`[Sync-Update] Local update done on ${currentNode}. Syncing to MAIN...`);
            const syncSql = 'CALL `stadvdb-mco2`.distributed_update(?, ?, ?, ?, ?, ?)';
            
            const syncLogId = await RecoveryManager.logTransaction('UPDATE', 'MAIN', syncSql, params);

            try {
                await db.query(syncSql, params);
                if (syncLogId) await RecoveryManager.updateLogStatus(syncLogId, 'COMMITTED');
            } catch (mainError) {
                console.error(`[Sync-Update] Failed to sync to MAIN: ${mainError.message}`);
                if (syncLogId) await RecoveryManager.updateLogStatus(syncLogId, 'FAILED', mainError.message);
            }
        }

        res.json({ success: true, data: results });

    } catch (dbError) {
        if (transactionId) await RecoveryManager.updateLogStatus(transactionId, 'FAILED', dbError.message);
        throw dbError;
    }
    // --- RECOVERY LOGIC END ---

  } catch (error) {
    console.error('Error in distributed_update:', error);
    res.status(500).json({ success: false, message: 'Error', error: error.message });
  }
});

// Distributed delete system
app.post('/api/titles/distributed-delete', async (req, res) => {
  try {
    const { tconst } = req.body;
    if (!tconst) return res.status(400).json({ success: false, message: 'tconst is required' });

    // --- RECOVERY LOGIC START (YAZAN) ---
    const currentNode = process.env.DB_NAME; 
    const isMainNode = currentNode === 'stadvdb-mco2';

    // 1. Define SQL
    const sql = 'CALL distributed_delete(?)';
    const params = [tconst];

    // 2. Log Primary
    // For deletes, Main broadcasts to everyone. Nodes just delete locally.
    const transactionId = await RecoveryManager.logTransaction('DELETE', isMainNode ? 'MAIN' : 'LOCAL', sql, params);

    try {
      // 3. Execute Primary (Local) Write
      const [results] = await db.query(sql, params);
      if (transactionId) await RecoveryManager.updateLogStatus(transactionId, 'COMMITTED');

      // 4. SYNC UPWARD (Node A/B -> Main)
      if (!isMainNode) {
          console.log(`[Sync-Delete] Local delete done on ${currentNode}. Syncing to MAIN...`);
          const syncSql = 'CALL `stadvdb-mco2`.distributed_delete(?)';
          
          const syncLogId = await RecoveryManager.logTransaction('DELETE', 'MAIN', syncSql, params);

          try {
              await db.query(syncSql, params);
              if (syncLogId) await RecoveryManager.updateLogStatus(syncLogId, 'COMMITTED');
          } catch (mainError) {
              console.error(`[Sync-Delete] Failed to sync to MAIN: ${mainError.message}`);
              if (syncLogId) await RecoveryManager.updateLogStatus(syncLogId, 'FAILED', mainError.message);
          }
      }

      res.json({ success: true, data: results });

    } catch (dbError) {
      if (transactionId) await RecoveryManager.updateLogStatus(transactionId, 'FAILED', dbError.message);
      throw dbError;
    }
    // --- RECOVERY LOGIC END ---

  } catch (error) {
    console.error('Error in distributed_delete:', error);
    res.status(500).json({ success: false, message: 'Error', error: error.message });
  }
});

// 3. READ-ONLY ROUTES (No Logging Needed)
// Distributed select system
app.get('/api/titles/distributed-select', async (req, res) => {
  try {
    const { select_column = 'averageRating', order_direction = 'DESC', limit_count = 10 } = req.query;
    const [results] = await db.query('CALL distributed_select(?, ?, ?)', [select_column, order_direction, parseInt(limit_count)]);
    res.json({
      success: true,
      count: results[0]?.length || 0,
      data: results[0] || []
    });
  } catch (error) {
    console.error('Error in distributed_select:', error);
    res.status(500).json({
      success: false,
      message: 'Error in distributed_select',
      error: error.message
    });
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
    const [results] = await db.query('CALL distributed_aggregation()');
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
    const {
      title_query,
      min_rating,
      max_rating,
      min_votes,
      result_limit = 10
    } = req.query;

    const query = `
      SELECT 
        tf.tconst,
        tf.primaryTitle AS title,
        tf.averageRating,
        tf.numVotes,
        tb.genres
      FROM title_ft AS tf
      LEFT JOIN title_basics AS tb ON tf.tconst = tb.tconst
      WHERE 
        (? IS NULL OR tf.primaryTitle LIKE CONCAT('%', ?, '%'))
        AND (? IS NULL OR tf.averageRating >= ?)
        AND (? IS NULL OR tf.averageRating <= ?)
        AND (? IS NULL OR tf.numVotes >= ?)
      ORDER BY 
        tf.numVotes DESC
      LIMIT ?
    `;

    const params = [
      title_query || null,
      title_query || null,
      min_rating || null,
      min_rating || null,
      max_rating || null,
      max_rating || null,
      min_votes || null,
      min_votes || null,
      parseInt(result_limit)
    ];

    const [results] = await db.query(query, params);
    res.json({
      success: true,
      count: results.length,
      filters: {
        title_query,
        min_rating,
        max_rating,
        min_votes,
        result_limit
      },
      data: results
    });
  } catch (error) {
    console.error('Error searching titles:', error);
    res.status(500).json({
      success: false,
      message: 'Error searching titles',
      error: error.message
    });
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
// START SERVER
// ============================================================================

app.listen(PORT, () => {
  console.log(`🚀 Server is running on http://localhost:${PORT}`);
  console.log(`📊 API endpoints available at http://localhost:${PORT}/api`);
});