import { useState, useEffect } from 'react';
import './App.css';

function App() {
  // Edit popup state
  const [editRow, setEditRow] = useState(null);
  const [showEditPopup, setShowEditPopup] = useState(false);

  const handleEditClick = (row) => {
    setEditRow(row);
    setShowEditPopup(true);
  };

  const handleEditPopupClose = () => {
    setShowEditPopup(false);
    setEditRow(null);
  };

  const [aggregations, setAggregations] = useState(null);
  const [loadingAggregations, setLoadingAggregations] = useState(true);
  const [aggError, setAggError] = useState(null);

  // Distributed search state
  const [searchTerm, setSearchTerm] = useState('');
  const [limit, setLimit] = useState(20);
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [hasSearched, setHasSearched] = useState(false);

  // Distributed select state
  const [selectColumn, setSelectColumn] = useState('averageRating');
  const [orderDirection, setOrderDirection] = useState('DESC');
  const [selectLimit, setSelectLimit] = useState(10);
  const [selectResults, setSelectResults] = useState([]);
  const [selectLoading, setSelectLoading] = useState(false);
  const [selectError, setSelectError] = useState(null);
  const [hasSelected, setHasSelected] = useState(false);

  useEffect(() => {
    fetchAggregations();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const fetchAggregations = () => {
    setLoadingAggregations(true);
    setAggError(null);
    fetch('http://localhost:5000/api/aggregation')
      .then((response) => response.json())
      .then((data) => {
        setAggregations(data.data);
        setLoadingAggregations(false);
        console.log('Aggregation results:', data);
      })
      .catch((error) => {
        setAggError('Failed to load statistics');
        setLoadingAggregations(false);
        console.error('Aggregation fetch error:', error);
      });
  };

  const handleSearch = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setHasSearched(true);

    try {
      const res = await fetch(
        `http://localhost:5000/api/titles/distributed-search?search_term=${encodeURIComponent(
          searchTerm
        )}&limit_count=${limit}`
      );
      let data;
      let text;
      try {
        data = await res.clone().json();
      } catch (jsonErr) {
        text = await res.text();
        console.error('Failed to parse JSON.');
        console.error('Status:', res.status, res.statusText);
        console.error('Headers:', Array.from(res.headers.entries()));
        console.error('Raw response:', text);
        setError('Search failed: Invalid server response. See console for details.');
        return;
      }
      if (data.success) {
        setResults(data.data);
      } else {
        setError(data.message || 'Search failed');
        console.error('API error:', data);
      }
    } catch (err) {
      console.error('Network or fetch error:', err);
      setError('Search failed');
    } finally {
      setLoading(false);
    }
  };

  const handleSelect = async (e) => {
    e.preventDefault();
    setSelectLoading(true);
    setSelectError(null);
    setHasSelected(true);

    try {
      const res = await fetch(
        `http://localhost:5000/api/titles/distributed-select?select_column=${encodeURIComponent(
          selectColumn
        )}&order_direction=${orderDirection}&limit_count=${selectLimit}`
      );
      const data = await res.json();
      if (data.success) {
        setSelectResults(data.data);
      } else {
        setSelectError(data.message || 'Select failed');
      }
    } catch (err) {
      console.error(err);
      setSelectError('Select failed');
    } finally {
      setSelectLoading(false);
    }
  };

  return (
    <div className="App app-root">
      <header className="app-header">
        <div className="app-header-inner">
          <div>
            <h1 className="app-title">Distributed IMDB Dashboard</h1>
            <p className="app-subtitle">
              Monitor stats and run distributed search / select queries from a single interface.
            </p>
          </div>
        </div>
      </header>

      <main className="app-main">
        {/* Database Statistics */}
        <section className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">📊 Database Statistics</h2>
              <p className="card-subtitle">
                High-level aggregation across your distributed nodes.
              </p>
            </div>
            <button className="btn btn-ghost" onClick={fetchAggregations}>
              Refresh
            </button>
          </div>

          {aggError && <div className="alert alert-error">{aggError}</div>}

          {loadingAggregations ? (
            <p className="text-muted">Loading statistics…</p>
          ) : aggregations ? (
            <div className="stats-grid">
              {Object.entries(aggregations).map(([key, value]) => (
                <div key={key} className="stat-card">
                  <div className="stat-label">
                    {key
                      .replace(/_/g, ' ')
                      .replace(/\b\w/g, (l) => l.toUpperCase())}
                  </div>
                  <div className="stat-value">
                    {typeof value === 'number'
                      ? value.toLocaleString(undefined, {
                          maximumFractionDigits: 4,
                        })
                      : value}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-muted">No statistics available.</p>
          )}
        </section>

        {/* Distributed Search System */}
        <section className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">🔎 Distributed Search</h2>
              <p className="card-subtitle">
                Search titles across all nodes using a unified endpoint.
              </p>
            </div>
          </div>

          <form className="form-row" onSubmit={handleSearch}>
            <div className="form-group flex-2">
              <label className="field-label">Search term</label>
              <input
                type="text"
                className="input"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                placeholder="Enter title keyword…"
              />
            </div>

            <div className="form-group">
              <label className="field-label">Limit</label>
              <input
                type="number"
                className="input"
                value={limit}
                min={1}
                max={100}
                onChange={(e) => setLimit(Number(e.target.value))}
              />
            </div>

            <div className="form-group align-end">
              <button type="submit" className="btn btn-primary">
                {loading ? 'Searching…' : 'Search'}
              </button>
            </div>
          </form>

          {error && <div className="alert alert-error">{error}</div>}

          <div className="table-wrapper">
            {loading && <p className="text-muted">Loading results…</p>}

            {!loading && !hasSearched && (
              <p className="empty-state">Run a search to see results here.</p>
            )}

            {!loading && hasSearched && results.length === 0 && !error && (
              <p className="empty-state">No results found.</p>
            )}

            {!loading && results.length > 0 && (
              <table className="data-table">
                <thead>
                  <tr>
                    {Object.keys(results[0]).map((col, idx) => (
                      <th key={idx}>{col}</th>
                    ))}
                    <th>Edit</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((row, i) => (
                    <tr key={i}>
                      {Object.values(row).map((val, j) => (
                        <td key={j}>{String(val)}</td>
                      ))}
                      <td>
                        <button
                          className="btn btn-secondary"
                          onClick={() => handleEditClick(row)}
                        >
                          Edit
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>

        {/* Edit Popup */}
        {showEditPopup && editRow && (
          <div
            className="modal-overlay"
            style={{
              position: 'fixed',
              top: 0,
              left: 0,
              width: '100vw',
              height: '100vh',
              background: 'rgba(0,0,0,0.4)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              zIndex: 1000,
            }}
          >
            <div
              className="modal-content"
              style={{
                background: '#fff',
                padding: 32,
                borderRadius: 8,
                minWidth: 350,
                maxWidth: 500,
                boxShadow: '0 2px 16px rgba(0,0,0,0.2)',
              }}
            >
              <h3 style={{ marginTop: 0 }}>Edit Entry</h3>
              <form>
                {Object.entries(editRow).map(([key, value]) => (
                  <div key={key} style={{ marginBottom: 16 }}>
                    <label
                      style={{
                        display: 'block',
                        fontWeight: 500,
                        marginBottom: 4,
                      }}
                    >
                      {key}
                    </label>
                    <input
                      type="text"
                      value={value}
                      disabled={key === 'tconst'}
                      style={{
                        width: '100%',
                        padding: 8,
                        borderRadius: 4,
                        border: '1px solid #bbb',
                        background: key === 'tconst' ? '#eee' : '#fff',
                      }}
                      readOnly={key === 'tconst'}
                    />
                  </div>
                ))}
                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'flex-end',
                    gap: 8,
                  }}
                >
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={handleEditPopupClose}
                  >
                    Close
                  </button>
                  {/* Save button can be added here in the future */}
                </div>
              </form>
            </div>
          </div>
        )}

        {/* Distributed Select System */}
        <section className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">📋 Distributed Select</h2>
              <p className="card-subtitle">
                Run ordered selects (TOP N) over distributed title data.
              </p>
            </div>
          </div>

          <form className="form-row" onSubmit={handleSelect}>
            <div className="form-group">
              <label className="field-label">Column</label>
              <select
                className="select"
                value={selectColumn}
                onChange={(e) => setSelectColumn(e.target.value)}
              >
                <option value="averageRating">averageRating</option>
                <option value="numVotes">numVotes</option>
                <option value="startYear">startYear</option>
              </select>
            </div>

            <div className="form-group">
              <label className="field-label">Order</label>
              <select
                className="select"
                value={orderDirection}
                onChange={(e) => setOrderDirection(e.target.value)}
              >
                <option value="DESC">DESC</option>
                <option value="ASC">ASC</option>
              </select>
            </div>

            <div className="form-group">
              <label className="field-label">Limit</label>
              <input
                type="number"
                className="input"
                value={selectLimit}
                min={1}
                max={100}
                onChange={(e) => setSelectLimit(Number(e.target.value))}
              />
            </div>

            <div className="form-group align-end">
              <button type="submit" className="btn btn-primary">
                {selectLoading ? 'Running…' : 'Select'}
              </button>
            </div>
          </form>

          {selectError && <div className="alert alert-error">{selectError}</div>}

          <div className="table-wrapper">
            {selectLoading && <p className="text-muted">Loading results…</p>}

            {!selectLoading && !hasSelected && (
              <p className="empty-state">Run a select query to see results here.</p>
            )}

            {!selectLoading &&
              hasSelected &&
              selectResults.length === 0 &&
              !selectError && <p className="empty-state">No results found.</p>}

            {!selectLoading && selectResults.length > 0 && (
              <table className="data-table">
                <thead>
                  <tr>
                    {Object.keys(selectResults[0]).map((col, idx) => (
                      <th key={idx}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {selectResults.map((row, i) => (
                    <tr key={i}>
                      {Object.values(row).map((val, j) => (
                        <td key={j}>{String(val)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
      </main>
    </div>
  );
}

export default App;
