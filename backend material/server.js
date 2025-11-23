const express = require('express');
const cors = require('cors');
require('dotenv').config();
const db = require('./db');

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// ============================================================================
// API ROUTES
// ============================================================================

// Test route
app.get('/api/test', (req, res) => {
  res.json({ message: 'Backend API is working!' });
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
        tf.numVotes
      FROM title_ft AS tf
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
          ROW_NUMBER() OVER (
            PARTITION BY t.startYear
            ORDER BY t.averageRating DESC
          ) AS rnk
        FROM title_ft t
        WHERE t.averageRating IS NOT NULL
          AND t.numVotes > ?
          AND t.startYear > ?
          AND t.startYear < ?
      )
      SELECT
        rt.startYear,
        rt.primaryTitle,
        rt.averageRating AS highest
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
