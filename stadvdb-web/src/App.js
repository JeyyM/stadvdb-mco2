import { useState, useEffect } from 'react';
import './App.css';

function App() {
  // Title Search System state
  const [titleQuery, setTitleQuery] = useState('Matrix');
  const [minRating, setMinRating] = useState(5.0);
  const [maxRating, setMaxRating] = useState(9.9);
  const [minVotes, setMinVotes] = useState(10000);
  const [resultLimit, setResultLimit] = useState(10);
  const [searchResults, setSearchResults] = useState([]);
  const [loadingSearch, setLoadingSearch] = useState(false);
  const [errorSearch, setErrorSearch] = useState(null);

  // Top Movies by Year state
  const [startYear, setStartYear] = useState(2000);
  const [endYear, setEndYear] = useState(2020);
  const [minVotesYear, setMinVotesYear] = useState(1000);
  const [topMoviesByYear, setTopMoviesByYear] = useState([]);

  const [loadingTopMovies, setLoadingTopMovies] = useState(false);
  const [errorTopMovies, setErrorTopMovies] = useState(null);

  // Function to search titles
  const handleSearch = () => {
    setLoadingSearch(true);
    setErrorSearch(null);

    const params = new URLSearchParams({
      title_query: titleQuery,
      min_rating: minRating,
      max_rating: maxRating,
      min_votes: minVotes,
      result_limit: resultLimit
    });

    fetch(`http://localhost:5000/api/titles/search-advanced?${params}`)
      .then(response => {
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        return response.json();
      })
      .then(data => {
        setSearchResults(data.data);
        setLoadingSearch(false);
      })
      .catch(error => {
        setErrorSearch(error.message);
        setLoadingSearch(false);
      });
  };

  // Function to search top movies by year
  const handleTopMoviesSearch = () => {
    setLoadingTopMovies(true);
    setErrorTopMovies(null);

    const params = new URLSearchParams({
      start_year: startYear,
      end_year: endYear,
      min_votes: minVotesYear
    });

    fetch(`http://localhost:5000/api/titles/top-by-year?${params}`)
      .then(response => {
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        return response.json();
      })
      .then(data => {
        setTopMoviesByYear(data.data);
        setLoadingTopMovies(false);
      })
      .catch(error => {
        setErrorTopMovies(error.message);
        setLoadingTopMovies(false);
      });
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>STADVDB IMDB Database</h1>
        
        {/* Title Search System Section */}
        <div style={{ 
          width: '90%', 
          marginTop: '40px',
          padding: '20px',
          backgroundColor: '#1e1e1e',
          borderRadius: '8px',
          border: '1px solid #444'
        }}>
          <h2>Title Search System</h2>
          <p style={{ fontSize: '14px', color: '#aaa', marginBottom: '20px' }}>
            Search titles with filters: title, rating range, and minimum votes
          </p>

          {/* Search Form */}
          <div style={{ 
            display: 'grid', 
            gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
            gap: '15px',
            marginBottom: '20px'
          }}>
            <div>
              <label style={{ display: 'block', marginBottom: '5px', fontSize: '14px' }}>
                Title Query:
              </label>
              <input
                type="text"
                value={titleQuery}
                onChange={(e) => setTitleQuery(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px',
                  borderRadius: '4px',
                  border: '1px solid #555',
                  backgroundColor: '#2a2a2a',
                  color: 'white',
                  fontSize: '14px'
                }}
                placeholder="e.g., Matrix"
              />
            </div>

            <div>
              <label style={{ display: 'block', marginBottom: '5px', fontSize: '14px' }}>
                Min Rating:
              </label>
              <input
                type="number"
                step="0.1"
                value={minRating}
                onChange={(e) => setMinRating(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px',
                  borderRadius: '4px',
                  border: '1px solid #555',
                  backgroundColor: '#2a2a2a',
                  color: 'white',
                  fontSize: '14px'
                }}
              />
            </div>

            <div>
              <label style={{ display: 'block', marginBottom: '5px', fontSize: '14px' }}>
                Max Rating:
              </label>
              <input
                type="number"
                step="0.1"
                value={maxRating}
                onChange={(e) => setMaxRating(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px',
                  borderRadius: '4px',
                  border: '1px solid #555',
                  backgroundColor: '#2a2a2a',
                  color: 'white',
                  fontSize: '14px'
                }}
              />
            </div>

            <div>
              <label style={{ display: 'block', marginBottom: '5px', fontSize: '14px' }}>
                Min Votes:
              </label>
              <input
                type="number"
                value={minVotes}
                onChange={(e) => setMinVotes(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px',
                  borderRadius: '4px',
                  border: '1px solid #555',
                  backgroundColor: '#2a2a2a',
                  color: 'white',
                  fontSize: '14px'
                }}
              />
            </div>

            <div>
              <label style={{ display: 'block', marginBottom: '5px', fontSize: '14px' }}>
                Result Limit:
              </label>
              <input
                type="number"
                value={resultLimit}
                onChange={(e) => setResultLimit(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px',
                  borderRadius: '4px',
                  border: '1px solid #555',
                  backgroundColor: '#2a2a2a',
                  color: 'white',
                  fontSize: '14px'
                }}
              />
            </div>
          </div>

          <button
            onClick={handleSearch}
            style={{
              padding: '12px 30px',
              fontSize: '16px',
              fontWeight: 'bold',
              backgroundColor: '#61dafb',
              color: '#282c34',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              marginBottom: '20px'
            }}
          >
            Search Titles
          </button>

          {/* Search Results */}
          {loadingSearch && <p>Searching...</p>}

          {errorSearch && (
            <div style={{ color: 'red' }}>
              <p>Error: {errorSearch}</p>
            </div>
          )}

          {!loadingSearch && !errorSearch && searchResults.length > 0 && (
            <div>
              <h3>Results ({searchResults.length})</h3>
              <div style={{ 
                maxHeight: '500px', 
                overflowY: 'auto',
                marginTop: '15px'
              }}>
                <table style={{
                  width: '100%',
                  borderCollapse: 'collapse',
                  fontSize: '14px'
                }}>
                  <thead>
                    <tr style={{ backgroundColor: '#2a2a2a' }}>
                      <th style={{ padding: '10px', textAlign: 'left', borderBottom: '2px solid #444' }}>ID</th>
                      <th style={{ padding: '10px', textAlign: 'left', borderBottom: '2px solid #444' }}>Title</th>
                      <th style={{ padding: '10px', textAlign: 'left', borderBottom: '2px solid #444' }}>Rating</th>
                      <th style={{ padding: '10px', textAlign: 'left', borderBottom: '2px solid #444' }}>Votes</th>
                      <th style={{ padding: '10px', textAlign: 'left', borderBottom: '2px solid #444' }}>Genres</th>
                    </tr>
                  </thead>
                  <tbody>
                    {searchResults.map((title, index) => (
                      <tr key={index} style={{ borderBottom: '1px solid #333' }}>
                        <td style={{ padding: '10px' }}>{title.tconst}</td>
                        <td style={{ padding: '10px', fontWeight: 'bold' }}>{title.title}</td>
                        <td style={{ padding: '10px' }}>{title.averageRating}</td>
                        <td style={{ padding: '10px' }}>{title.numVotes.toLocaleString()}</td>
                        <td style={{ padding: '10px', fontSize: '12px' }}>{title.genres || 'N/A'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {!loadingSearch && !errorSearch && searchResults.length === 0 && (
            <p style={{ color: '#aaa' }}>No results found. Try adjusting your filters.</p>
          )}
        </div>

        {/* Top Movies of a Time Period Section */}
        <div style={{ 
          width: '90%', 
          marginTop: '40px',
          padding: '20px',
          backgroundColor: '#1e1e1e',
          borderRadius: '8px',
          border: '1px solid #444'
        }}>
          <h2>Top Movies of a Time Period</h2>
          <p style={{ fontSize: '14px', color: '#aaa', marginBottom: '20px' }}>
            Find the highest-rated title for each year within a date range
          </p>

          {/* Year Range Form */}
          <div style={{ 
            display: 'grid', 
            gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
            gap: '15px',
            marginBottom: '20px'
          }}>
            <div>
              <label style={{ display: 'block', marginBottom: '5px', fontSize: '14px' }}>
                Start Year:
              </label>
              <input
                type="number"
                value={startYear}
                onChange={(e) => setStartYear(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px',
                  borderRadius: '4px',
                  border: '1px solid #555',
                  backgroundColor: '#2a2a2a',
                  color: 'white',
                  fontSize: '14px'
                }}
              />
            </div>

            <div>
              <label style={{ display: 'block', marginBottom: '5px', fontSize: '14px' }}>
                End Year:
              </label>
              <input
                type="number"
                value={endYear}
                onChange={(e) => setEndYear(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px',
                  borderRadius: '4px',
                  border: '1px solid #555',
                  backgroundColor: '#2a2a2a',
                  color: 'white',
                  fontSize: '14px'
                }}
              />
            </div>

            <div>
              <label style={{ display: 'block', marginBottom: '5px', fontSize: '14px' }}>
                Min Votes:
              </label>
              <input
                type="number"
                value={minVotesYear}
                onChange={(e) => setMinVotesYear(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px',
                  borderRadius: '4px',
                  border: '1px solid #555',
                  backgroundColor: '#2a2a2a',
                  color: 'white',
                  fontSize: '14px'
                }}
              />
            </div>
          </div>

          <button
            onClick={handleTopMoviesSearch}
            style={{
              padding: '12px 30px',
              fontSize: '16px',
              fontWeight: 'bold',
              backgroundColor: '#61dafb',
              color: '#282c34',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              marginBottom: '20px'
            }}
          >
            Find Top Movies
          </button>

          {/* Results */}
          {loadingTopMovies && <p>Loading top movies...</p>}

          {errorTopMovies && (
            <div style={{ color: 'red' }}>
              <p>Error: {errorTopMovies}</p>
            </div>
          )}

          {!loadingTopMovies && !errorTopMovies && topMoviesByYear.length > 0 && (
            <div>
              <h3>Top Movie Per Year ({topMoviesByYear.length} years)</h3>
              <div style={{ 
                maxHeight: '500px', 
                overflowY: 'auto',
                marginTop: '15px'
              }}>
                <table style={{
                  width: '100%',
                  borderCollapse: 'collapse',
                  fontSize: '14px'
                }}>
                  <thead>
                    <tr style={{ backgroundColor: '#2a2a2a' }}>
                      <th style={{ padding: '10px', textAlign: 'left', borderBottom: '2px solid #444' }}>Year</th>
                      <th style={{ padding: '10px', textAlign: 'left', borderBottom: '2px solid #444' }}>Title</th>
                      <th style={{ padding: '10px', textAlign: 'left', borderBottom: '2px solid #444' }}>Rating</th>
                      <th style={{ padding: '10px', textAlign: 'left', borderBottom: '2px solid #444' }}>Genres</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topMoviesByYear.map((movie, index) => (
                      <tr key={index} style={{ borderBottom: '1px solid #333' }}>
                        <td style={{ padding: '10px', fontWeight: 'bold' }}>{movie.startYear}</td>
                        <td style={{ padding: '10px' }}>{movie.primaryTitle}</td>
                        <td style={{ padding: '10px', color: '#61dafb' }}>{movie.highest}</td>
                        <td style={{ padding: '10px', fontSize: '12px' }}>{movie.genres || 'N/A'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {!loadingTopMovies && !errorTopMovies && topMoviesByYear.length === 0 && (
            <p style={{ color: '#aaa' }}>No results found. Try adjusting your year range.</p>
          )}
        </div>

      </header>
    </div>
  );
}

export default App;
