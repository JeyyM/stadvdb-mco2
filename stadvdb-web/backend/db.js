const mysql = require('mysql2');
require('dotenv').config();

/// Create a connection pool 
const pool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME || 'stadvdb-mco2',
  port: process.env.DB_PORT || 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Promisify the pool for async/await
const promisePool = pool.promise();

// Test the connection
pool.getConnection((err, connection) => {
  if (err) {
    console.error('Error connecting to MySQL database:', err.message);
    return;
  }
  console.log(`Successfully connected to MySQL database: ${process.env.DB_NAME}`);
  connection.release();
});

module.exports = promisePool;
