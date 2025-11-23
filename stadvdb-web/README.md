# STADVDB IMDB Database Web Application

A full-stack web application for querying and analyzing IMDB movie data using React and Node.js with MySQL.

## Project Structure

```
stadvdb-web/
├── backend/               # Express.js API server
│   ├── server.js         # Main server file with API routes
│   ├── db.js             # MySQL database connection
│   ├── package.json      # Backend dependencies
│   ├── kill-port.js      # Utility to free port 5000
│   └── .env.example      # Environment variables template
├── src/                  # React frontend
│   └── App.js            # Main React component
└── public/               # Static files
```

## Features

### 1. Title Search System
- Search movies by title
- Filter by rating range (min/max)
- Filter by minimum votes
- Limit number of results
- Display genres for each title

### 2. Top Movies by Year
- Find the highest-rated movie for each year in a date range
- Filter by minimum vote count
- View genres for top movies

## Prerequisites

- Node.js (v14 or higher)
- MySQL Server (v8 or higher)
- IMDB database loaded into MySQL with tables:
  - `title_ft` (title full-text table)
  - `title_basics` (title information including genres)
  - `title_ratings` (ratings information)

## Database Setup

1. Make sure your MySQL database `stadvdb-mco2` is created
2. Run the SQL scripts in this order:
   - `IMDB Data Loaders/Title Basics Import.sql`
   - `IMDB Data Loaders/Title Ratings Import.sql`
   - `Schema Builders/6. create title_ft.sql`

## Backend Setup

1. Navigate to the backend directory:
   ```powershell
   cd stadvdb-web\backend
   ```

2. Install dependencies:
   ```powershell
   npm install
   ```

3. Create a `.env` file based on `.env.example`:
   ```powershell
   copy .env.example .env
   ```

4. Edit the `.env` file with your MySQL credentials:
   ```
   DB_HOST=localhost
   DB_USER=root
   DB_PASSWORD=your_actual_password
   DB_NAME=stadvdb-mco2
   DB_PORT=3306
   PORT=5000
   ```

5. Start the backend server:
   ```powershell
   npm start
   ```

   The server will run on `http://localhost:5000`

## Frontend Setup

1. Navigate to the main stadvdb-web directory:
   ```powershell
   cd stadvdb-web
   ```

2. Install dependencies:
   ```powershell
   npm install
   ```

3. Start the React development server:
   ```powershell
   npm start
   ```

   The app will open in your browser at `http://localhost:3000`

## Running the Full Application

You need to run both servers simultaneously:

1. **Terminal 1** - Backend:
   ```powershell
   cd stadvdb-web\backend
   npm start
   ```

2. **Terminal 2** - Frontend:
   ```powershell
   cd stadvdb-web
   npm start
   ```

## API Endpoints

### GET `/api/test`
Test endpoint to verify the API is working.

### GET `/api/titles/search-advanced`
Search titles with multiple filters.

**Query Parameters:**
- `title_query` (string): Search term for title
- `min_rating` (float): Minimum rating (0-10)
- `max_rating` (float): Maximum rating (0-10)
- `min_votes` (integer): Minimum number of votes
- `result_limit` (integer): Maximum results to return (default: 10)

**Example:**
```
http://localhost:5000/api/titles/search-advanced?title_query=Matrix&min_rating=7&min_votes=10000&result_limit=10
```

### GET `/api/titles/top-by-year`
Get the top-rated movie for each year in a range.

**Query Parameters:**
- `start_year` (integer): Starting year (default: 2000)
- `end_year` (integer): Ending year (default: 2020)
- `min_votes` (integer): Minimum votes threshold (default: 1000)

**Example:**
```
http://localhost:5000/api/titles/top-by-year?start_year=2010&end_year=2020&min_votes=5000
```

## Troubleshooting

### Backend won't start - "Port 5000 already in use"
Run the kill-port script manually:
```powershell
node kill-port.js
```

### Database connection error
1. Verify MySQL is running
2. Check your `.env` file credentials
3. Ensure the `stadvdb-mco2` database exists
4. Verify the required tables are loaded

### Frontend can't connect to backend
1. Make sure the backend is running on port 5000
2. Check for CORS errors in browser console
3. Verify the API URL in `App.js` is `http://localhost:5000`

### No results returned
1. Verify your database has data loaded
2. Check the browser console for errors
3. Try relaxing your filter criteria

## SQL Query Details

### Title Search Query
Joins `title_ft` with `title_basics` to include genre information:
```sql
SELECT 
  tf.tconst,
  tf.primaryTitle AS title,
  tf.averageRating,
  tf.numVotes,
  tb.genres
FROM title_ft AS tf
LEFT JOIN title_basics AS tb ON tf.tconst = tb.tconst
WHERE ...
```

### Top Movies by Year Query
Uses a CTE with ROW_NUMBER() window function to find the top movie per year:
```sql
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
  WHERE ...
)
SELECT * FROM ranked_titles WHERE rnk = 1
```

## Technologies Used

- **Frontend**: React 19, CSS
- **Backend**: Node.js, Express.js
- **Database**: MySQL 8, mysql2 driver
- **Other**: CORS, dotenv

## License

ISC
