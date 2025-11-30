const express = require('express');
const cors = require('cors');
require('dotenv').config({ path: process.env.DOTENV_CONFIG_PATH || '.env' });
const db = require('./db');

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// ============================================================================
// API ROUTES
// Distributed delete system
app.post('/api/titles/distributed-delete', async (req, res) => {
  let connection;
  try {
    const { tconst, sleepSeconds = 0, isolationLevel = 'READ COMMITTED'} = req.body;
    if (!tconst) {
      return res.status(400).json({ success: false, message: 'tconst is required' });
    }

    connection = await db.getConnection();

    await connection.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolationLevel}`);

    const [results] = await connection.query('CALL distributed_delete(?)', [tconst, sleepSeconds]);
    res.json({ success: true, data: results });
  } catch (error) {
    console.error('Error in distributed_delete:', error);
    if (error.code === 'ER_LOCK_DEADLOCK') {
      res.status(409).json({ success: false, message: 'Deadlock detected' });
    } else {
      res.status(500).json({ success: false, message: error.message });
    }
  } finally {
    if (connection) connection.release();
  }
});

// Distributed update system
app.post('/api/titles/distributed-update', async (req, res) => {
  let connection;
  try {
    const {
      tconst,
      primaryTitle,
      runtimeMinutes,
      averageRating,
      numVotes,
      startYear,
      isolationLevel = 'READ COMMITTED',
      sleepSeconds = 0
    } = req.body;

    // Validate required fields
    if (!tconst) {
      return res.status(400).json({ success: false, message: 'tconst is required' });
    }

    connection = await db.getConnection();

    await connection.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolationLevel}`);

    // Call the stored procedure (6 args)
    const [results] = await connection.query(
      'CALL distributed_update(?, ?, ?, ?, ?, ?, ?)',
      [
        tconst,
        primaryTitle,
        runtimeMinutes,
        averageRating,
        numVotes,
        startYear,
        sleepSeconds
      ]
    );
    res.json({ success: true, data: results, isolationLevelUsed: isolationLevel });
  } catch (error) {
    if (error.code === 'ER_LOCK_DEADLOCK') {
      res.status(409).json({ success: false, message: 'Deadlock detected', code: 'ER_LOCK_DEADLOCK' });
    } else {
      res.status(500).json({ success: false, message: 'Error in distributed_update', error: error.message });
    }
  } finally {
    if (connection) connection.release();
  }
});
// ============================================================================

// Distributed select system
app.get('/api/titles/distributed-select', async (req, res) => {
  let connection;
  try {
    const { select_column = 'averageRating', order_direction = 'DESC', limit_count = 10, isolationLevel = 'READ COMMITTED'} = req.query;

    connection = await db.getConnection();
    await connection.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolationLevel}`);

    const [results] = await connection.query('CALL distributed_select(?, ?, ?)', [select_column, order_direction, parseInt(limit_count)]);
    res.json({
      success: true,
      count: results[0]?.length || 0,
      data: results[0] || []
    });
  } catch (error) {
    console.error('Error in distributed_select:', error);
    res.status(500).json({ success: false, message: error.message });
  } finally {
    if (connection) connection.release();
  }
});
// Distributed search system
app.get('/api/titles/distributed-search', async (req, res) => {
  let connection;
  try {
    const {
      search_term = '',
      limit_count = 20,
      isolationLevel = 'READ COMMITTED'
    } = req.query;

    connection = await db.getConnection();

    await connection.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolationLevel}`);

    const [results] = await connection.query(
        'CALL distributed_search(?, ?)',
        [search_term, parseInt(limit_count)]
    );

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
  } finally {
    if (connection) connection.release();
  }
});

// Test route
app.get('/api/test', (req, res) => {
  res.json({ message: 'Backend API is working!' });
});


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

    // Validate required fields (6 args, weightedRating is now calculated in SQL)
    if (!tconst || !primaryTitle || runtimeMinutes === undefined || averageRating === undefined || numVotes === undefined || startYear === undefined) {
      return res.status(400).json({ success: false, message: 'All fields are required' });
    }

    // Call the stored procedure (6 args)
    const [results] = await db.query(
      'CALL distributed_insert(?, ?, ?, ?, ?, ?)',
      [
        tconst,
        primaryTitle,
        runtimeMinutes,
        averageRating,
        numVotes,
        startYear
      ]
    );
    res.json({ success: true, data: results });
  } catch (error) {
    console.error('Error in distributed_insert:', error);
    res.status(500).json({
      success: false,
      message: 'Error in distributed_insert',
      error: error.message
    });
  }
});

// Title search system
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

// ============================================================================
// START SERVER
// ============================================================================

app.listen(PORT, () => {
  console.log(`🚀 Server is running on http://localhost:${PORT}`);
  console.log(`📊 API endpoints available at http://localhost:${PORT}/api`);
});
